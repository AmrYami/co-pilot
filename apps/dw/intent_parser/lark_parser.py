from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from lark import Lark, Token, Transformer, v_args

GRAMMAR = r"""
?start: expr

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

fts_clause: FTS_VERB LPAREN token_list RPAREN   -> fts_group
          | FTS_VERB token_list                 -> fts_group

token_list: value (_token_sep value)*

_token_sep: OR | PIPE | COMMA

eq_clause: alias comparator value_list          -> eq_clause
         | alias EQ_OP value_list               -> eq_clause
         | alias HAS value_list                 -> eq_clause
         | alias IS value_list                  -> eq_clause

value_list: value (_value_sep value)*

_value_sep: OR | COMMA

value: VALUE       -> value_token

in_clause: alias IN LPAREN value_list RPAREN     -> in_clause

comparison_clause: alias COMP_SIGN number        -> comp_sign_clause
                 | alias COMP_WORD number        -> comp_word_clause
                 | alias BETWEEN number AND number -> between_clause

alias: ALIAS        -> alias_token

number: NUMBER      -> number_token

AND: /(?i:\band\b)/
OR: /(?i:\bor\b)/
PIPE: "|"
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
ALIAS: /(?i:(departments?|department|stakeholders?|stakeholder|owner|vat|email|entity|request\s*type|contract_status|contract\s*status|contract_id|contract\s*id|contractor_id|requester|department_oul))/
VALUE: /[^(),|]+?(?=\s+(?i:and|or)\b|\)|,|$)/
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

    # ---- literal cleaners -------------------------------------------------

    def alias_token(self, token: Token) -> str:
        return self._clean_identifier(token)

    def value_token(self, token: Token) -> str:
        return self._clean_value(token)

    def number_token(self, token: Token) -> Any:
        text = str(token)
        return self._normalize_number(text)

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
        alias_norm = alias.upper()
        if self.allowed_columns and alias_norm not in self.allowed_columns:
            clause = {"type": "unknown", "alias": alias_norm, "values": values}
            self.clauses.append(clause)
            return clause
        self.eq_filters.append([alias_norm, values])
        clause = {"type": "eq", "col": alias_norm, "values": values}
        self.clauses.append(clause)
        return clause

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


class DwQuestionParser:
    """Parse NL DW questions to a normalized intent via Lark grammar."""

    def __init__(self) -> None:
        self._parser = Lark(GRAMMAR, parser="lalr", propagate_positions=False, maybe_placeholders=False)

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
        question_text = re.sub(r"(?i)^and\s+", "", question_text)

        tree = self._parser.parse(question_text)
        transformer = _DwTransformer(alias_map=alias_map, allowed_columns=allowed_columns)
        bool_tree = transformer.transform(tree)
        return ParsedIntent(
            eq_filters=transformer.eq_filters,
            num_filters=transformer.num_filters,
            fts_tokens=transformer.fts_tokens,
            bool_tree=bool_tree,
            clauses=transformer.clauses,
        )
