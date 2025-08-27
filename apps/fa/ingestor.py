"""
Prefix-aware FrontAccounting schema ingestor.

Reads FA tables from a single MySQL/MariaDB database where each tenant/business
uses a numeric prefix like "2_", "3_" before canonical FA table names.

Writes canonicalized metadata into the Postgres memory DB (mem_* tables):
  - mem_snapshots
  - mem_tables (table_name = canonical name, schema_name = prefix)
  - mem_columns
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
import time
import datetime as _dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine, Row


@dataclass
class IngestOptions:
    fa_version: Optional[str] = None
    limit_tables: int = 0  # 0 = no cap


def _json_default(o):
    if isinstance(o, (_dt.datetime, _dt.date, _dt.time)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (bytes, bytearray, memoryview)):
        b = bytes(o)
        try:
            return b.decode("utf-8")
        except Exception:
            return base64.b64encode(b).decode("ascii")
    return str(o)


class FASchemaIngestor:
    def __init__(
        self,
        fa_engine: Engine,
        mem_engine: Engine,
        prefix_regex: str = r"^[0-9]+_$",
        sample_rows_per_table: int = 5,
        profile_stats: bool = False,
        namespace_prefix: str = "fa::",
    ) -> None:
        self.fa = fa_engine
        self.mem = mem_engine
        self.prefix_regex = re.compile(prefix_regex)
        self.sample_rows = max(0, int(sample_rows_per_table))
        self.profile_stats = profile_stats
        self.ns_prefix = namespace_prefix

    # ----------------------------
    # Public API
    # ----------------------------
    def ingest_prefix(self, prefix: str, fa_version: Optional[str] = None, limit_tables: int = 0) -> int:
        """Ingests one prefix and returns the new mem_snapshots.id.
        Skips write if schema hash didn't change (but still returns last snapshot id).
        """
        self._validate_prefix(prefix)
        t0 = time.time()

        # 1) Introspect FA schema
        tables = self._fetch_tables(prefix, limit_tables)
        cols = self._fetch_columns(prefix)
        pk_map = self._derive_primary_keys(cols)
        fk_map = self._fetch_foreign_keys(prefix)

        # 2) Assemble canonical metadata with optional samples/stats
        canonical = self._canon_metadata(prefix, tables, cols, pk_map, fk_map)
        if self.sample_rows > 0:
            self._attach_samples(prefix, canonical, self.sample_rows)

        # 3) Compute schema hash
        schema_hash = self._compute_schema_hash(canonical)

        # 4) Upsert into Postgres
        namespace = f"{self.ns_prefix}{prefix}"
        snapshot_id = self._write_snapshot_and_metadata(namespace, schema_hash, canonical, fa_version)

        dt = (time.time() - t0) * 1000
        print(f"[ingestor] prefix={prefix} tables={len(canonical)} snapshot_id={snapshot_id} in {dt:.0f}ms")
        return snapshot_id

    def ingest_prefixes(self, prefixes: Iterable[str], fa_version: Optional[str] = None, limit_tables: int = 0) -> Dict[str, int]:
        ids: Dict[str, int] = {}
        for p in prefixes:
            ids[p] = self.ingest_prefix(p, fa_version=fa_version, limit_tables=limit_tables)
        return ids

    # ----------------------------
    # Introspection helpers (MySQL)
    # ----------------------------
    def _validate_prefix(self, prefix: str) -> None:
        if not self.prefix_regex.match(prefix):
            raise ValueError(f"Invalid prefix '{prefix}'. Expected pattern {self.prefix_regex.pattern}")

    def _fetch_tables(self, prefix: str, limit_tables: int) -> List[Row]:
        sql = text(
            """
            SELECT  TABLE_NAME, ENGINE, TABLE_COLLATION, TABLE_COMMENT, TABLE_ROWS
            FROM    INFORMATION_SCHEMA.TABLES
            WHERE   TABLE_SCHEMA = DATABASE()
              AND   LEFT(TABLE_NAME, :preflen) = :prefix
            ORDER BY TABLE_NAME
            """
        )
        if limit_tables and limit_tables > 0:
            sql = text(sql.text + "\nLIMIT :lim")
        with self.fa.connect() as c:
            params = {"prefix": prefix, "preflen": len(prefix)}
            if limit_tables and limit_tables > 0:
                params["lim"] = limit_tables
            res = c.execute(sql, params)
            return list(res.fetchall())

    def _fetch_columns(self, prefix: str) -> List[Row]:
        sql = text(
            """
            SELECT  TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT,
                    COLUMN_KEY
            FROM    INFORMATION_SCHEMA.COLUMNS
            WHERE   TABLE_SCHEMA = DATABASE()
              AND   LEFT(TABLE_NAME, :preflen) = :prefix
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
        )
        with self.fa.connect() as c:
            return list(c.execute(sql, {"prefix": prefix, "preflen": len(prefix)}).fetchall())

    def _fetch_foreign_keys(self, prefix: str) -> Dict[str, List[Tuple[str, str, str]]]:
        sql = text(
            """
            SELECT  kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
            FROM    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                   AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                   AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE   kcu.TABLE_SCHEMA = DATABASE()
              AND   LEFT(kcu.TABLE_NAME, :preflen) = :prefix
              AND   tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION
            """
        )
        out: Dict[str, List[Tuple[str, str, str]]] = {}
        with self.fa.connect() as c:
            for r in c.execute(sql, {"prefix": prefix, "preflen": len(prefix)}):
                out.setdefault(r[0], []).append((r[1], r[2], r[3]))
        return out

    def _derive_primary_keys(self, cols: List[Row]) -> Dict[str, List[str]]:
        pk: Dict[str, List[str]] = {}
        for r in cols:
            table, col, colkey = r[0], r[1], r[9]
            if colkey == "PRI":
                pk.setdefault(table, []).append(col)
        return pk

    # ----------------------------
    # Canonicalization & sampling
    # ----------------------------
    def _canon_metadata(
        self,
        prefix: str,
        tables: List[Row],
        cols: List[Row],
        pk_map: Dict[str, List[str]],
        fk_map: Dict[str, List[Tuple[str, str, str]]],
    ) -> Dict[str, Dict[str, Any]]:
        by_table: Dict[str, Dict[str, Any]] = {}
        # group columns
        cols_by_table: Dict[str, List[Row]] = {}
        for r in cols:
            cols_by_table.setdefault(r[0], []).append(r)

        for t in tables:
            raw = t[0]
            canonical = raw[len(prefix):]
            pk_cols = pk_map.get(raw, [])
            fks = [
                {
                    "column": c,
                    "ref_table": rt[len(prefix):] if rt and rt.startswith(prefix) else rt,
                    "ref_column": rc,
                }
                for (c, rt, rc) in fk_map.get(raw, [])
            ]
            columns = []
            for c in cols_by_table.get(raw, []):
                col_meta = {
                    "column_name": c[1],
                    "data_type": c[2],
                    "is_nullable": (c[3] == "YES"),
                    "default_value": c[4],
                    "max_length": c[5],
                    "numeric_precision": c[6],
                    "numeric_scale": c[7],
                    "comment": c[8],
                    "is_primary": c[1] in pk_cols,
                    "is_foreign": any(c[1] == fk[0] for fk in fk_map.get(raw, [])),
                }
                columns.append(col_meta)

            date_cols = [
                c[1] for c in cols_by_table.get(raw, [])
                if c[2] in ("date", "datetime", "timestamp") or "date" in c[1].lower()
            ]

            by_table[canonical] = {
                "raw_name": raw,
                "canonical": canonical,
                "row_count": (int(t[4]) if t[4] is not None else None),  # TABLE_ROWS
                "engine_name": t[1],         # ENGINE
                "collation_name": t[2],      # TABLE_COLLATION
                "table_comment": t[3],       # TABLE_COMMENT
                "primary_key": pk_cols,
                "date_columns": date_cols,
                "fks": fks,
                "columns": columns,
                "sample_rows": None,
            }
        return by_table

    def _attach_samples(self, prefix: str, meta: Dict[str, Dict[str, Any]], limit: int) -> None:
        if limit <= 0:
            return
        with self.fa.connect() as c:
            for canon, tmeta in meta.items():
                raw = tmeta["raw_name"]
                sql = text(f"SELECT * FROM `{raw}` LIMIT :n")
                try:
                    raw_rows = c.execute(sql, {"n": limit}).mappings().all()
                    rows = [json.loads(json.dumps(r, default=_json_default)) for r in raw_rows]
                except Exception:
                    rows = []  # perms / blobs / etc.
                tmeta["sample_rows"] = rows

    # ----------------------------
    # Hashing & writes (Postgres)
    # ----------------------------
    def _compute_schema_hash(self, meta: Dict[str, Dict[str, Any]]) -> str:
        def stable(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: stable(obj[k]) for k in sorted(obj.keys())}
            if isinstance(obj, list):
                return [stable(x) for x in obj]
            return obj

        payload = json.dumps(stable(meta), separators=(",", ":"), ensure_ascii=False, default=_json_default)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _write_snapshot_and_metadata(
        self,
        namespace: str,
        schema_hash: str,
        meta: Dict[str, Dict[str, Any]],
        fa_version: Optional[str],
    ) -> int:
        with self.mem.begin() as tx:
            # Find last snapshot
            last = tx.exec_driver_sql(
                "SELECT id, schema_hash FROM mem_snapshots WHERE namespace=%s ORDER BY id DESC LIMIT 1",
                (namespace,),
            ).fetchone()
            if last and last[1] == schema_hash:
                return int(last[0])

            diff_from = last[0] if last else None
            snap_id = tx.exec_driver_sql(
                "INSERT INTO mem_snapshots(namespace, schema_hash, diff_from) VALUES (%s, %s, %s) RETURNING id",
                (namespace, schema_hash, diff_from),
            ).scalar_one()

            # Upsert tables & columns (use psycopg2 %s paramstyle consistently)
            for canon, t in meta.items():
                table_id = tx.exec_driver_sql(
                    """
                    INSERT INTO mem_tables(
                        namespace, snapshot_id, table_name, schema_name, row_count,
                        engine_name, collation_name, table_comment,
                        primary_key, sample_rows, date_columns
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (namespace, table_name, schema_name)
                    DO UPDATE SET
                        snapshot_id     = EXCLUDED.snapshot_id,
                        row_count       = EXCLUDED.row_count,
                        engine_name     = EXCLUDED.engine_name,
                        collation_name  = EXCLUDED.collation_name,
                        table_comment   = EXCLUDED.table_comment,
                        primary_key     = EXCLUDED.primary_key,
                        sample_rows     = EXCLUDED.sample_rows,
                        date_columns    = EXCLUDED.date_columns,
                        updated_at      = NOW()
                    RETURNING id
                    """,
                    (
                        namespace,
                        snap_id,
                        canon,
                        # store prefix in schema_name
                        t["raw_name"][: len(t["raw_name"]) - len(canon)],
                        t["row_count"],
                        t["engine_name"],
                        t["collation_name"],
                        t["table_comment"],
                        json.dumps(t["primary_key"], default=_json_default),
                        json.dumps(t.get("sample_rows"), default=_json_default),
                        json.dumps(t.get("date_columns"), default=_json_default),
                    ),
                ).scalar_one()

                # Upsert columns
                existing_cols = tx.exec_driver_sql(
                    "SELECT id, column_name FROM mem_columns WHERE namespace=%s AND table_id=%s",
                    (namespace, table_id),
                ).fetchall()
                existing_set = {r[1] for r in existing_cols}

                for cmeta in t["columns"]:
                    tx.exec_driver_sql(
                        """
                        INSERT INTO mem_columns(namespace, table_id, column_name, data_type, is_nullable, default_value,
                                                max_length, numeric_precision, numeric_scale, is_primary, is_foreign)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (namespace, table_id, column_name)
                        DO UPDATE SET
                            data_type = EXCLUDED.data_type,
                            is_nullable = EXCLUDED.is_nullable,
                            default_value = EXCLUDED.default_value,
                            max_length = EXCLUDED.max_length,
                            numeric_precision = EXCLUDED.numeric_precision,
                            numeric_scale = EXCLUDED.numeric_scale,
                            is_primary = EXCLUDED.is_primary,
                            is_foreign = EXCLUDED.is_foreign,
                            updated_at = NOW()
                        """,
                        (
                            namespace,
                            table_id,
                            cmeta["column_name"],
                            cmeta["data_type"],
                            cmeta["is_nullable"],
                            cmeta["default_value"],
                            cmeta["max_length"],
                            cmeta["numeric_precision"],
                            cmeta["numeric_scale"],
                            cmeta["is_primary"],
                            cmeta["is_foreign"],
                        ),
                    )

                # Delete columns that disappeared
                to_drop = existing_set - {c["column_name"] for c in t["columns"]}
                if to_drop:
                    tx.exec_driver_sql(
                        "DELETE FROM mem_columns WHERE namespace=%s AND table_id=%s AND column_name = ANY(%s)",
                        (namespace, table_id, list(to_drop)),
                    )

            # Store FA version hint (optional)
            if fa_version:
                tx.exec_driver_sql(
                    """
                    INSERT INTO mem_settings(namespace, key, value, value_type, scope, updated_by)
                    VALUES (%s, 'FA_VERSION', %s::jsonb, 'string', 'namespace', 'ingestor')
                    ON CONFLICT DO NOTHING
                    """,
                    (namespace, json.dumps(fa_version)),
                )

            return int(snap_id)
