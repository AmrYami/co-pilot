from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from lark import Lark, Token, Transformer, v_args

GRAMMAR = r"""
?start: aggregator_sentence
     | expr

?expr: expr OR term   -> or_expr
     | term

?term: term AND factor   -> and_expr
     | factor

?factor: LPAREN expr RPAREN   -> group
       | clause

?clause: fts_clause
       | comparison_clause
       | eq_clause
       | in_clause

aggregator_sentence: for_filter CLAUSE_BREAK aggregate_phrase trailing_filters   -> aggregator_sentence
                   | for_filter CLAUSE_BREAK aggregate_phrase                    -> aggregator_sentence
                   | for_filter COMMA aggregate_phrase trailing_filters          -> aggregator_sentence
                   | for_filter COMMA aggregate_phrase                           -> aggregator_sentence
                   | for_filter aggregate_phrase trailing_filters                -> aggregator_sentence
                   | for_filter aggregate_phrase                                 -> aggregator_sentence
                   | aggregate_phrase trailing_filters                           -> aggregator_sentence
                   | aggregate_phrase                                            -> aggregator_sentence

trailing_filters: (COMMA? for_filter)+

aggregate_phrase: COMMAND? aggregate_list group_by_tail?       -> aggregate_phrase

aggregate_list: aggregate_term (AND aggregate_term)*           -> aggregate_list

aggregate_term: AGG_TOTAL                                     -> total_term
               | AGG_COUNT                                    -> count_term

fts_clause: FTS_VERB LPAREN token_list RPAREN   -> fts_group
          | FTS_VERB token_list                 -> fts_group

token_list: value (_token_sep value)*

_token_sep: OR | PIPE | COMMA

eq_clause: alias comparator value_list          -> eq_clause
         | alias EQ_OP value_list               -> eq_clause
         | alias HAS value_list                 -> eq_clause
         | alias IS value_list                  -> eq_clause

for_filter: FOR alias (EQ_OP value_list | value_list) -> for_filter

group_by_tail: GROUPED_BY alias -> group_by_clause
             | BY alias         -> group_by_clause

value_list: value (_value_sep value)*

_value_sep: OR | COMMA

value: value_atom value_paren? -> value_with_paren

in_clause: alias IN LPAREN value_list RPAREN     -> in_clause

comparison_clause: alias COMP_SIGN number        -> comp_sign_clause
                 | alias COMP_WORD number        -> comp_word_clause
                 | alias BETWEEN number AND number -> between_clause

alias: ALIAS        -> alias_token
     | QUOTED_ALIAS -> quoted_alias_token

number: NUMBER      -> number_token

value_atom: VALUE       -> value_token
          | ALIAS       -> alias_value_token
          | QUOTED_ALIAS -> quoted_alias_value_token

value_paren: LPAREN value_atom RPAREN -> value_paren_token

AND: /(?i:\band\b)/
OR: /(?i:\bor\b)/
PIPE: "|"
CLAUSE_BREAK: /,\s*(?=(?:total|sum|count)\b)/i
COMMA: ","
HAS: /(?i:(has|include|includes|including|with))/
IS: /(?i:(is|equals|equal\s+to))/
EQ_OP: "="
comparator: /(?i:(equals|equal\s+to|is))/
IN: /(?i:in)/
BETWEEN: /(?i:between)/
FTS_VERB: /(?i:(contains|including|mentioning|about|with|search\s+for))/
COMP_SIGN: ">=" | "<=" | ">" | "<"
COMP_WORD: /(?i:(greater\s+than\s+or\s+equal\s+to|greater\s+than|more\s+than|above|over|at\s+least|less\s+than\s+or\s+equal\s+to|less\s+than|under|below|at\s+most|no\s+less\s+than|no\s+more\s+than))/
FOR: /(?i:for)/
COMMAND: /(?i:(show|list|display|give|provide|report|present))/
GROUPED_BY: /(?i:group(?:ed)?\s+by)/
BY: /(?i:by)/
AGG_TOTAL.1: /(?i:total(?:\s+(?:amount|value))?|sum(?:\s+(?:amount|value))?)/
AGG_COUNT.1: /(?i:count(?:\s*\(\*\))?|count(?:\s+of)?(?:\s+contracts)?|contract\s+count|number\s+of\s+contracts)/

QUOTED_ALIAS.7: /(?i:(["'])[A-Z0-9_]+(?:\s+[A-Z0-9_]+)*["'])/
ALIAS.6: /(?i:(?!FOR\b)(?!TOTAL\b)(?!COUNT\b)(?!AND\b)(?!OR\b)[A-Z0-9_]+(?:\s+[A-Z0-9_]+)*)/
VALUE.5: /(?i:(?!and\b)(?!or\b)(?!total\b)(?!sum\b)(?!count\b))[^\s(),|=][^(),|=]*?(?=\s+(?i:(?:or)\b|(?:and\b\s+(?:\"[A-Z0-9_ ]+\"|[A-Z0-9_]+(?:\s+[A-Z0-9_]+)*)\s*=)|\s*(?:total|sum|count)\b)|\)|,|$)/
NUMBER: /[-+]?\d+(?:\.\d+)?/
LPAREN: "("
RPAREN: ")"

%import common.WS_INLINE
%ignore WS_INLINE
"""

COMPARISON_WORD_MAP = {
    "greater than or equal to": "gte",
    "greater than": "gt",
    "more than": "gt",
    "above": "gt",
    "over": "gt",
    "at least": "gte",
    "less than or equal to": "lte",
    "at most": "lte",
    "less than": "lt",
    "under": "lt",
    "below": "lt",
    "no less than": "gte",
    "no more than": "lte",
}

COMPARISON_SIGN_MAP = {
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
}


@dataclass
class ParsedIntent:
    eq_filters: List[List[Any]]
    num_filters: List[Dict[str, Any]]
    fts_tokens: List[str]
    bool_tree: Optional[Dict[str, Any]]
    clauses: List[Dict[str, Any]]
    aggregations: List[Dict[str, Any]] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_hint: Optional[Dict[str, Any]] = None


@v_args(inline=True)
class _DwTransformer(Transformer):
    def __init__(
        self,
        alias_map: Optional[Dict[str, Sequence[str]]] = None,
        allowed_columns: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__()
        self.alias_map = {str(k or "").strip().upper(): list(v or []) for k, v in (alias_map or {}).items()}
        if allowed_columns:
            self.allowed_columns = {str(c or "").strip().upper() for c in allowed_columns}
        else:
            self.allowed_columns = None
        self.eq_filters: List[List[Any]] = []
        self.num_filters: List[Dict[str, Any]] = []
        self.fts_tokens: List[str] = []
        self.clauses: List[Dict[str, Any]] = []
        self.aggregations: List[Dict[str, Any]] = []
        self.group_by_cols: List[str] = []
        self.order_hint: Optional[Dict[str, Any]] = None

    # ---- literal cleaners -------------------------------------------------

    def alias_token(self, token: Token) -> str:
        return self._clean_identifier(token)

    def quoted_alias_token(self, token: Token) -> str:
        text = token.value if isinstance(token, Token) else str(token)
        stripped = text.strip()
        if len(stripped) >= 2 and stripped[0] in {'"', "'"} and stripped[-1] == stripped[0]:
            stripped = stripped[1:-1]
        return self._clean_identifier(stripped)

    def value_token(self, token: Token) -> str:
        return self._clean_value(token)

    def alias_value_token(self, token: Token) -> str:
        return self._clean_value(token)

    def quoted_alias_value_token(self, token: Token) -> str:
        return self._clean_value(token)

    def number_token(self, token: Token) -> Any:
        text = str(token)
        return self._normalize_number(text)

    def value_paren_token(self, _lp: Token, inner: Any, _rp: Token) -> str:
        text = inner if not isinstance(inner, Token) else self._clean_value(inner)
        return f"({text})"

    def value_with_paren(self, atom: Any, paren: Optional[Any] = None) -> str:
        base = atom if not isinstance(atom, Token) else self._clean_value(atom)
        if paren:
            suffix = paren if not isinstance(paren, Token) else self._clean_value(paren)
            return f"{base} {suffix}"
        return base

    def comparator(self, token: Token) -> str:
        return "eq"

    # ---- list helpers -----------------------------------------------------

    def value_list(self, first: Any, *rest: Any) -> List[str]:
        values = [first, *rest]
        cleaned: List[str] = []
        for v in values:
            if isinstance(v, Token):
                continue
            current = self._clean_value(v)
            if current:
                cleaned.append(current)
        return cleaned

    def token_list(self, first: Any, *rest: Any) -> List[str]:
        values = [first, *rest]
        cleaned: List[str] = []
        for v in values:
            if isinstance(v, Token):
                continue
            current = self._clean_value(v)
            if current:
                cleaned.append(current)
        return cleaned

    # ---- clause handlers --------------------------------------------------

    def fts_group(self, *items: Any) -> Dict[str, Any]:
        tokens: List[str] = []
        for item in items:
            if isinstance(item, list):
                tokens = item
        normalized = self._normalize_token_list(tokens)
        for token in normalized:
            if token not in self.fts_tokens:
                self.fts_tokens.append(token)
        clause = {"type": "fts", "tokens": normalized}
        self.clauses.append(clause)
        return clause

    def eq_clause(self, alias: str, *args: Any) -> Dict[str, Any]:
        values_node = None
        for item in reversed(args):
            if isinstance(item, list):
                values_node = item
                break
            if not isinstance(item, Token):
                values_node = [item]
                break
        values = self._flatten_values(values_node or [])
        return self._append_eq_filter(alias.upper(), values)

    def in_clause(self, alias: str, values: List[str]) -> Dict[str, Any]:
        return self.eq_clause(alias, values)

    def comp_sign_clause(self, alias: str, comp_token: Token, number: Any) -> Dict[str, Any]:
        op = COMPARISON_SIGN_MAP.get(str(comp_token), "eq")
        return self._register_numeric_clause(alias, op, [number])

    def comp_word_clause(self, alias: str, word_token: Token, number: Any) -> Dict[str, Any]:
        op = self._normalize_comparison_word(str(word_token))
        return self._register_numeric_clause(alias, op, [number])

    def between_clause(self, alias: str, first: Any, _and_tok: Token, second: Any) -> Dict[str, Any]:
        return self._register_numeric_clause(alias, "between", [first, second])

    def group(self, node: Any) -> Any:
        return node

    def aggregator_sentence(self, *items: Any) -> Dict[str, Any]:
        clause: Dict[str, Any] = {"type": "aggregate", "aggregations": [], "group_by": []}
        for item in items:
            if isinstance(item, dict):
                aggs = item.get("aggregations")
                if aggs:
                    for agg in aggs:
                        self._register_aggregation(
                            agg.get("func"),
                            agg.get("column"),
                            agg.get("alias"),
                            bool(agg.get("distinct")),
                        )
                    clause["aggregations"].extend(aggs)
                group_by = item.get("group_by")
                if group_by:
                    self._register_group_by(group_by)
        clause["group_by"] = list(self.group_by_cols)
        self.clauses.append(clause)
        return clause

    def aggregate_phrase(self, *items: Any) -> Dict[str, Any]:
        aggregations: List[Dict[str, Any]] = []
        group_by: Optional[str] = None
        for item in items:
            if isinstance(item, dict):
                if item.get("aggregations"):
                    aggregations.extend(item["aggregations"])
                if item.get("group_by") and not group_by:
                    group_by = item["group_by"]
        result: Dict[str, Any] = {}
        if aggregations:
            result["aggregations"] = aggregations
        if group_by:
            result["group_by"] = group_by
        return result

    def aggregate_list(self, first: Any, *rest: Any) -> Dict[str, Any]:
        aggregations: List[Dict[str, Any]] = []
        for item in (first, *rest):
            if isinstance(item, Token):
                continue
            if isinstance(item, dict) and item.get("aggregations"):
                aggregations.extend(item["aggregations"])
        return {"aggregations": aggregations}

    def total_term(self, token: Token) -> Dict[str, Any]:
        return {
            "aggregations": [
                {
                    "func": "SUM",
                    "column": "CONTRACT_VALUE_NET_OF_VAT",
                    "alias": "TOTAL_AMOUNT",
                    "distinct": False,
                }
            ]
        }

    def count_term(self, token: Token) -> Dict[str, Any]:
        return {
            "aggregations": [
                {
                    "func": "COUNT",
                    "column": "*",
                    "alias": "TOTAL_COUNT",
                    "distinct": False,
                }
            ]
        }

    def group_by_clause(self, *items: Any) -> Dict[str, Any]:
        alias = None
        for item in items:
            if isinstance(item, Token):
                continue
            alias = item
        if alias is None:
            return {"group_by": ""}
        return {"group_by": str(alias).upper()}

    def command(self, token: Token) -> None:  # noqa: D401 - ignored command token
        return None

    def for_filter(self, *items: Any) -> Dict[str, Any]:
        alias = None
        values_node = None
        for item in items:
            if isinstance(item, Token):
                if item.type in {"FOR", "EQ_OP"}:
                    continue
            if alias is None:
                alias = item
            else:
                values_node = item
                break
        alias_norm = str(alias).strip().upper() if alias is not None else ""
        values = self._flatten_values(values_node or [])
        return self._append_eq_filter(alias_norm, values)

    def and_expr(self, *items: Any) -> Dict[str, Any]:
        nodes = [item for item in items if not isinstance(item, Token)]
        if not nodes:
            return {}
        result = nodes[0]
        for node in nodes[1:]:
            result = self._combine_bool("and", result, node)
        return result

    def or_expr(self, *items: Any) -> Dict[str, Any]:
        nodes = [item for item in items if not isinstance(item, Token)]
        if not nodes:
            return {}
        result = nodes[0]
        for node in nodes[1:]:
            result = self._combine_bool("or", result, node)
        return result

    # ---- final ------------------------------------------------------------

    def transform(self, tree):
        result = super().transform(tree)
        return result

    # ---- helpers ----------------------------------------------------------

    def _flatten_values(self, values: Any) -> List[str]:
        if isinstance(values, list):
            return [self._clean_value(v) for v in values if self._clean_value(v)]
        cleaned = self._clean_value(values)
        return [cleaned] if cleaned else []

    def _register_numeric_clause(self, alias: str, op: str, numbers: List[Any]) -> Dict[str, Any]:
        alias_norm = alias.upper()
        values = [self._normalize_number(n) for n in numbers]
        clause = {"type": "num", "col": alias_norm, "op": op, "values": values}
        self.clauses.append(clause)
        self.num_filters.append({"col": alias_norm, "op": op, "values": values})
        return clause

    def _combine_bool(self, kind: str, left: Any, right: Any) -> Dict[str, Any]:
        children: List[Any] = []

        def _extend(node: Any) -> None:
            if isinstance(node, dict) and node.get("type") == kind and "children" in node:
                children.extend(node["children"])
            else:
                children.append(node)

        _extend(left)
        _extend(right)
        clause = {"type": kind, "children": children}
        self.clauses.append(clause)
        return clause

    def _clean_value(self, value: Any) -> str:
        text = str(value) if not isinstance(value, Token) else value.value
        cleaned = text.strip(" \"'\t\n\r")
        return re.sub(r"\s+", " ", cleaned).strip()

    def _clean_identifier(self, token: Token) -> str:
        text = token.value if isinstance(token, Token) else str(token)
        cleaned = re.sub(r"\s+", " ", text.strip(" \"'\t\n\r"))
        return cleaned.upper()

    def _normalize_number(self, value: Any) -> Any:
        text = str(value)
        text = text.replace(",", "").strip()
        try:
            if "." in text:
                return float(text)
            return int(text)
        except ValueError:
            return text

    def _normalize_comparison_word(self, text: str) -> str:
        lower = re.sub(r"\s+", " ", text.lower()).strip()
        return COMPARISON_WORD_MAP.get(lower, "eq")

    def _normalize_token_list(self, tokens: List[str]) -> List[str]:
        normalized: List[str] = []
        for token in tokens:
            cleaned = self._clean_value(token)
            if not cleaned:
                continue
            parts = re.split(r"(?i)\s+or\s+|,|\|", cleaned)
            for part in parts:
                part_clean = part.strip().lower()
                if not part_clean or part_clean in {'or', 'and', '|', ','}:
                    continue
                normalized.append(part_clean)
        return normalized

    def _append_eq_filter(self, alias_norm: str, values: List[str]) -> Dict[str, Any]:
        if self.allowed_columns and alias_norm not in self.allowed_columns:
            clause = {"type": "unknown", "alias": alias_norm, "values": values}
            self.clauses.append(clause)
            return clause
        self.eq_filters.append([alias_norm, values])
        clause = {"type": "eq", "col": alias_norm, "values": values}
        self.clauses.append(clause)
        return clause

    def _register_aggregation(self, func: Optional[str], column: Optional[str], alias: Optional[str], distinct: bool) -> None:
        if not func or not column or not alias:
            return
        descriptor = {
            "func": str(func).upper(),
            "column": str(column).upper(),
            "alias": str(alias).upper(),
            "distinct": bool(distinct),
        }
        if descriptor not in self.aggregations:
            self.aggregations.append(descriptor)
        if not self.order_hint and descriptor["func"] == "COUNT" and self.group_by_cols:
            self.order_hint = {"col": self.group_by_cols[0], "desc": False}

    def _register_group_by(self, alias_norm: str) -> None:
        normalized = str(alias_norm or "").strip().upper()
        if not normalized:
            return
        if normalized not in self.group_by_cols:
            self.group_by_cols.append(normalized)
        if not self.order_hint:
            self.order_hint = {"col": normalized, "desc": False}


class DwQuestionParser:
    """Parse NL DW questions to a normalized intent via Lark grammar."""

    def __init__(self) -> None:
        self._parser = Lark(GRAMMAR, parser="lalr", propagate_positions=False, maybe_placeholders=False)
        self._agg_parser = Lark(
            GRAMMAR,
            parser="lalr",
            start="aggregator_sentence",
            propagate_positions=False,
            maybe_placeholders=False,
        )

    def parse(
        self,
        question: str,
        *,
        alias_map: Optional[Dict[str, Sequence[str]]] = None,
        allowed_columns: Optional[Sequence[str]] = None,
    ) -> ParsedIntent:
        question_text = question or ""
        match = re.search(r"(?i)\bwhere\b(.+)", question_text)
        if match:
            question_text = match.group(1)
        question_text = question_text.strip()
        question_text = re.sub(r"[.?!]+$", "", question_text)
        question_text = re.sub(r"(?i)^and\s+", "", question_text)
        agg_hint = bool(
            re.search(r"(?i)\b(total|sum|count)\b", question_text)
            and re.search(r"(?i)\bby\b", question_text)
        )
        if agg_hint:
            try:
                tree = self._agg_parser.parse(question_text)
            except Exception:
                tree = self._parser.parse(question_text)
        else:
            tree = self._parser.parse(question_text)
        transformer = _DwTransformer(alias_map=alias_map, allowed_columns=allowed_columns)
        bool_tree = transformer.transform(tree)
        aggregations = [dict(agg) for agg in transformer.aggregations]
        group_by = list(transformer.group_by_cols)
        order_hint = dict(transformer.order_hint) if transformer.order_hint else None
        return ParsedIntent(
            eq_filters=[
                [item[0], list(item[1])]
                if isinstance(item, (list, tuple)) and len(item) == 2
                else (list(item) if isinstance(item, (list, tuple)) else item)
                for item in transformer.eq_filters
            ],
            num_filters=[dict(entry) for entry in transformer.num_filters],
            fts_tokens=list(transformer.fts_tokens),
            bool_tree=bool_tree,
            clauses=list(transformer.clauses),
            aggregations=aggregations,
            group_by=group_by,
            order_hint=order_hint,
        )
