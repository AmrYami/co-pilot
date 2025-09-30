from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Sequence, Tuple

from dateutil.relativedelta import relativedelta


@dataclass
class BuiltSQL:
    sql: str
    binds: Dict[str, object]


def gross_expr() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
    )


def overlap_predicate(
    date_start_bind: str = ":date_start", date_end_bind: str = ":date_end"
) -> str:
    return (
        "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        f"AND START_DATE <= {date_end_bind} AND END_DATE >= {date_start_bind})"
    )


def request_date_between(
    date_start_bind: str = ":date_start", date_end_bind: str = ":date_end"
) -> str:
    return f"REQUEST_DATE BETWEEN {date_start_bind} AND {date_end_bind}"


def end_date_between(
    date_start_bind: str = ":date_start", date_end_bind: str = ":date_end"
) -> str:
    return f"END_DATE BETWEEN {date_start_bind} AND {date_end_bind}"


def _stakeholder_select(slot: int, where_clause: str) -> str:
    return (
        "SELECT CONTRACT_STAKEHOLDER_{slot} AS STK, {gross} AS GVAL, "
        "OWNER_DEPARTMENT AS OWNER_DEPT, CONTRACT_OWNER AS OWNER, CONTRACT_ID AS CONTRACT_ID "
        'FROM "Contract" {where}'
    ).format(slot=slot, gross=gross_expr(), where=where_clause)


def stakeholder_union(
    where_clause: str,
    *,
    slots: int = 8,
) -> str:
    selects = [_stakeholder_select(i, where_clause) for i in range(1, slots + 1)]
    return "\nUNION ALL\n".join(selects)


def sql_missing_contract_id() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        "WHERE CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID) = ''\n"
        'ORDER BY REQUEST_DATE DESC'
    )
    return BuiltSQL(sql=sql, binds={})


def sql_gross_by_stakeholder_slots(ds: date, de: date, *, slots: int = 8) -> BuiltSQL:
    where_clause = "WHERE " + overlap_predicate()
    union = stakeholder_union(where_clause, slots=slots)
    sql = (
        "WITH STAKEHOLDER_UNION AS (\n"
        f"{union}\n"
        ")\n"
        "SELECT\n"
        "  STK AS STAKEHOLDER,\n"
        "  SUM(GVAL) AS TOTAL_GROSS\n"
        "FROM STAKEHOLDER_UNION\n"
        "WHERE STK IS NOT NULL AND TRIM(STK) <> ''\n"
        "GROUP BY STK\n"
        "ORDER BY TOTAL_GROSS DESC"
    )
    return BuiltSQL(sql=sql, binds={"date_start": ds, "date_end": de})


def sql_top_gross_ytd(year: int, top_n: int, *, today: date) -> BuiltSQL:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), today)
    sql = (
        f"SELECT CONTRACT_ID, CONTRACT_OWNER, {gross_expr()} AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        "WHERE "
        + request_date_between()
        + "\nORDER BY TOTAL_GROSS DESC\n"
        "FETCH FIRST :top_n ROWS ONLY"
    )
    return BuiltSQL(
        sql=sql,
        binds={"date_start": start, "date_end": end, "top_n": top_n},
    )


def sql_avg_gross_by_request_type(ds: date, de: date) -> BuiltSQL:
    sql = (
        f"SELECT REQUEST_TYPE AS REQUEST_TYPE, AVG({gross_expr()}) AS AVG_GROSS\n"
        'FROM "Contract"\n'
        "WHERE "
        + request_date_between()
        + "\nGROUP BY REQUEST_TYPE\nORDER BY AVG_GROSS DESC"
    )
    return BuiltSQL(sql=sql, binds={"date_start": ds, "date_end": de})


def sql_monthly_trend_by_request_date(ds: date, de: date) -> BuiltSQL:
    sql = (
        "SELECT TRUNC(REQUEST_DATE, 'MM') AS MONTH_BUCKET, COUNT(*) AS CONTRACT_COUNT\n"
        'FROM "Contract"\n'
        "WHERE "
        + request_date_between()
        + "\nGROUP BY TRUNC(REQUEST_DATE, 'MM')\nORDER BY MONTH_BUCKET"
    )
    return BuiltSQL(sql=sql, binds={"date_start": ds, "date_end": de})


def sql_status_totals_for_entity_no(entity_no: str) -> BuiltSQL:
    sql = (
        f"SELECT CONTRACT_STATUS AS CONTRACT_STATUS, SUM({gross_expr()}) AS TOTAL_GROSS, COUNT(*) AS CONTRACT_COUNT\n"
        'FROM "Contract"\n'
        "WHERE ENTITY_NO = :entity_no\n"
        "GROUP BY CONTRACT_STATUS\n"
        "ORDER BY CONTRACT_COUNT DESC"
    )
    return BuiltSQL(sql=sql, binds={"entity_no": entity_no})


def sql_counts_30_60_90(now: date) -> BuiltSQL:
    buckets = [
        ("0-30", now, now + timedelta(days=30)),
        ("31-60", now + timedelta(days=31), now + timedelta(days=60)),
        ("61-90", now + timedelta(days=61), now + timedelta(days=90)),
    ]
    selects: List[str] = []
    binds: Dict[str, object] = {}
    for idx, (label, start, end) in enumerate(buckets, start=1):
        k_start = f"b{idx}_start"
        k_end = f"b{idx}_end"
        binds[k_start] = start
        binds[k_end] = end
        selects.append(
            "SELECT '{label}' AS BUCKET, COUNT(*) AS CONTRACT_COUNT FROM \"Contract\" "
            "WHERE {predicate}"
            .format(
                label=label,
                predicate=end_date_between(f":{k_start}", f":{k_end}"),
            )
        )
    sql = "\nUNION ALL\n".join(selects)
    return BuiltSQL(sql=sql, binds=binds)


def sql_owner_dept_highest_avg_gross(ds: date, de: date) -> BuiltSQL:
    sql = (
        f"SELECT OWNER_DEPARTMENT, AVG({gross_expr()}) AS AVG_GROSS\n"
        'FROM "Contract"\n'
        "WHERE "
        + request_date_between()
        + "\nGROUP BY OWNER_DEPARTMENT\n"
        "ORDER BY AVG_GROSS DESC\n"
        "FETCH FIRST 1 ROWS ONLY"
    )
    return BuiltSQL(sql=sql, binds={"date_start": ds, "date_end": de})


def sql_stakeholders_more_than_n_2024(n_min: int) -> BuiltSQL:
    ds = date(2024, 1, 1)
    de = date(2024, 12, 31)
    where_clause = "WHERE " + request_date_between()
    union = stakeholder_union(where_clause)
    sql = (
        "WITH STAKEHOLDER_HITS AS (\n"
        f"{union}\n"
        ")\n"
        "SELECT\n"
        "  STK AS STAKEHOLDER,\n"
        "  COUNT(*) AS CONTRACT_COUNT\n"
        "FROM STAKEHOLDER_HITS\n"
        "WHERE STK IS NOT NULL AND TRIM(STK) <> ''\n"
        "GROUP BY STK\n"
        "HAVING COUNT(*) > :min_count\n"
        "ORDER BY CONTRACT_COUNT DESC"
    )
    return BuiltSQL(
        sql=sql,
        binds={"date_start": ds, "date_end": de, "min_count": int(n_min)},
    )


def sql_missing_rep_email() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        "WHERE REPRESENTATIVE_EMAIL IS NULL "
        "OR TRIM(UPPER(REPRESENTATIVE_EMAIL)) IN ('', 'NA', 'N/A')\n"
        'ORDER BY REQUEST_DATE DESC'
    )
    return BuiltSQL(sql=sql, binds={})


def sql_requester_quarter_totals(requester: str) -> BuiltSQL:
    sql = (
        f"SELECT TRUNC(REQUEST_DATE, 'Q') AS QUARTER_START, SUM({gross_expr()}) AS TOTAL_GROSS, COUNT(*) AS CONTRACT_COUNT\n"
        'FROM "Contract"\n'
        "WHERE REQUESTER = :requester\n"
        "GROUP BY TRUNC(REQUEST_DATE, 'Q')\n"
        "ORDER BY QUARTER_START"
    )
    return BuiltSQL(sql=sql, binds={"requester": requester})


def sql_stakeholder_dept_2024() -> BuiltSQL:
    ds = date(2024, 1, 1)
    de = date(2024, 12, 31)
    where_clause = "WHERE " + request_date_between()
    union = stakeholder_union(where_clause)
    sql = (
        "WITH STAKEHOLDER_DEPTS AS (\n"
        f"{union}\n"
        ")\n"
        "SELECT\n"
        "  STK AS STAKEHOLDER,\n"
        "  COUNT(*) AS CONTRACT_COUNT,\n"
        "  SUM(GVAL) AS TOTAL_GROSS,\n"
        "  LISTAGG(DISTINCT OWNER_DEPT, ', ') WITHIN GROUP (ORDER BY OWNER_DEPT) AS DEPARTMENTS\n"
        "FROM STAKEHOLDER_DEPTS\n"
        "WHERE STK IS NOT NULL AND TRIM(STK) <> ''\n"
        "GROUP BY STK\n"
        "ORDER BY TOTAL_GROSS DESC"
    )
    return BuiltSQL(
        sql=sql,
        binds={"date_start": ds, "date_end": de},
    )


def sql_owner_stakeholder_pairs_top(ds: date, de: date, *, top_n: int) -> BuiltSQL:
    where_clause = "WHERE " + overlap_predicate()
    union = stakeholder_union(where_clause)
    sql = (
        "WITH PAIRS AS (\n"
        f"{union}\n"
        ")\n"
        "SELECT\n"
        "  OWNER AS CONTRACT_OWNER,\n"
        "  STK AS STAKEHOLDER,\n"
        "  SUM(GVAL) AS TOTAL_GROSS\n"
        "FROM PAIRS\n"
        "WHERE STK IS NOT NULL AND TRIM(STK) <> ''\n"
        "GROUP BY OWNER, STK\n"
        "ORDER BY TOTAL_GROSS DESC\n"
        "FETCH FIRST :top_n ROWS ONLY"
    )
    return BuiltSQL(
        sql=sql,
        binds={"date_start": ds, "date_end": de, "top_n": top_n},
    )


def sql_duplicate_contract_ids() -> BuiltSQL:
    sql = (
        "SELECT CONTRACT_ID, COUNT(*) AS DUP_COUNT\n"
        'FROM "Contract"\n'
        "WHERE CONTRACT_ID IS NOT NULL AND TRIM(CONTRACT_ID) <> ''\n"
        "GROUP BY CONTRACT_ID\n"
        "HAVING COUNT(*) > 1\n"
        "ORDER BY DUP_COUNT DESC"
    )
    return BuiltSQL(sql=sql, binds={})


def sql_median_gross_by_owner_dept_this_year(today: date) -> BuiltSQL:
    start = date(today.year, 1, 1)
    end = date(today.year, 12, 31)
    sql = (
        f"SELECT OWNER_DEPARTMENT, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {gross_expr()}) AS MEDIAN_GROSS\n"
        'FROM "Contract"\n'
        "WHERE "
        + request_date_between()
        + "\nGROUP BY OWNER_DEPARTMENT\n"
        "ORDER BY MEDIAN_GROSS DESC"
    )
    return BuiltSQL(sql=sql, binds={"date_start": start, "date_end": end})


def sql_end_before_start() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        "WHERE END_DATE < START_DATE\n"
        'ORDER BY REQUEST_DATE DESC'
    )
    return BuiltSQL(sql=sql, binds={})


def sql_duration_mismatch_12m() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        "WHERE REGEXP_SUBSTR(UPPER(DURATION), '([0-9]+)\\s*MONTH') = '12'\n"
        "  AND START_DATE IS NOT NULL AND END_DATE IS NOT NULL\n"
        "  AND ADD_MONTHS(START_DATE, 12) <> END_DATE\n"
        'ORDER BY START_DATE'
    )
    return BuiltSQL(sql=sql, binds={})


def sql_yoy(ds: date, de: date) -> BuiltSQL:
    prev_start = ds - relativedelta(years=1)
    prev_end = de - relativedelta(years=1)
    current_overlap = overlap_predicate(":ds", ":de")
    previous_overlap = overlap_predicate(":p_ds", ":p_de")
    sql = (
        f"SELECT 'CURRENT' AS PERIOD_LABEL, SUM({gross_expr()}) AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {current_overlap}\n"
        "UNION ALL\n"
        f"SELECT 'PRIOR' AS PERIOD_LABEL, SUM({gross_expr()}) AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {previous_overlap}"
    )
    return BuiltSQL(
        sql=sql,
        binds={"ds": ds, "de": de, "p_ds": prev_start, "p_de": prev_end},
    )


def sql_status_in_gross_threshold(statuses: Sequence[str], gross_min: float) -> BuiltSQL:
    placeholders = [f":status_{i}" for i in range(len(statuses))]
    status_in = ", ".join(placeholders)
    sql = (
        f"SELECT *, {gross_expr()} AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE CONTRACT_STATUS IN ({status_in})\n"
        f"  AND {gross_expr()} > :gross_min\n"
        "ORDER BY TOTAL_GROSS DESC"
    )
    binds: Dict[str, object] = {f"status_{i}": status for i, status in enumerate(statuses)}
    binds["gross_min"] = gross_min
    return BuiltSQL(sql=sql, binds=binds)


def sql_entity_top3_gross(ds: date, de: date) -> BuiltSQL:
    gross = gross_expr()
    sql = (
        "WITH RANKED AS (\n"
        f"  SELECT ENTITY, CONTRACT_ID, {gross} AS TOTAL_GROSS,\n"
        f"         ROW_NUMBER() OVER (PARTITION BY ENTITY ORDER BY {gross} DESC) AS RN\n"
        '  FROM "Contract"\n'
        "  WHERE "
        + request_date_between()
        + "\n)\n"
        "SELECT ENTITY, CONTRACT_ID, TOTAL_GROSS\n"
        "FROM RANKED\n"
        "WHERE RN <= 3\n"
        "ORDER BY ENTITY, RN"
    )
    return BuiltSQL(sql=sql, binds={"date_start": ds, "date_end": de})


def sql_owner_vs_oul_mismatch() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        "WHERE NVL(TRIM(OWNER_DEPARTMENT), '(NULL)') <> NVL(TRIM(DEPARTMENT_OUL), '(NULL)')\n"
        'ORDER BY REQUEST_DATE DESC'
    )
    return BuiltSQL(sql=sql, binds={})

