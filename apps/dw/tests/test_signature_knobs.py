import json
import pathlib
import sys

import pytest
from sqlalchemy import create_engine, text

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.learning_store import (  # noqa: E402
    load_rules_for_question,
    reset_signature_knobs_cache,
    reset_intent_match_log_cache,
    signature_variants,
)


@pytest.fixture(autouse=True)
def _reset_caches():
    reset_signature_knobs_cache()
    reset_intent_match_log_cache()
    yield
    reset_signature_knobs_cache()
    reset_intent_match_log_cache()


def _enable_knobs(monkeypatch, *, fts_shape=None, eq_list_mode=None):
    if fts_shape is not None:
        monkeypatch.setenv("SIG_FTS_SHAPE", fts_shape)
    else:
        monkeypatch.delenv("SIG_FTS_SHAPE", raising=False)
    if eq_list_mode is not None:
        monkeypatch.setenv("SIG_EQ_LIST_MODE", eq_list_mode)
    else:
        monkeypatch.delenv("SIG_EQ_LIST_MODE", raising=False)
    reset_signature_knobs_cache()


def test_signature_variants_adds_legacy_fallback(monkeypatch):
    intent = {
        "eq_filters": [["DEPARTMENT", ["SUPPORT SERVICES"]]],
        "fts_groups": [["home care"], ["maintenance"]],
    }

    _enable_knobs(monkeypatch, fts_shape=None, eq_list_mode=None)
    default_variant = signature_variants(intent)[0]

    _enable_knobs(monkeypatch, fts_shape="groups_only", eq_list_mode="any_len")
    variants = signature_variants(intent)

    assert len(variants) >= 2
    assert variants[-1] == default_variant


def test_any_len_signature_handles_list_length_changes(monkeypatch):
    intent_multi = {
        "eq_filters": [["STAKEHOLDER", ["Tamer Said", "U1835"]]],
    }
    intent_single = {
        "eq_filters": [["STAKEHOLDER", ["Tamer Said"]]],
    }

    _enable_knobs(monkeypatch, eq_list_mode="any_len")
    sig_multi = signature_variants(intent_multi)[0][2]
    sig_single = signature_variants(intent_single)[0][2]

    assert sig_multi == sig_single


def test_groups_only_ignores_token_sizes(monkeypatch):
    intent_a = {
        "fts_groups": [["home cares"], ["maintenance tasks"]],
    }
    intent_b = {
        "fts_groups": [["rent"], ["solar clinic requirements"]],
    }

    _enable_knobs(monkeypatch, fts_shape="groups_only")
    sig_a = signature_variants(intent_a)[0][2]
    sig_b = signature_variants(intent_b)[0][2]

    assert sig_a == sig_b


def test_load_rules_uses_legacy_signature(monkeypatch):
    base_intent = {
        "eq_filters": [["DEPARTMENT", ["SUPPORT SERVICES"]]],
        "fts_groups": [["home cares"], ["maintenance"]],
    }

    _enable_knobs(monkeypatch, fts_shape=None, eq_list_mode=None)
    default_sha256, default_sha1, default_sig = signature_variants(base_intent)[0]

    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as cx:
        cx.execute(
            text(
                """
                CREATE TABLE dw_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question_norm TEXT,
                    rule_kind TEXT,
                    rule_payload TEXT,
                    enabled BOOLEAN DEFAULT 1,
                    scope TEXT DEFAULT 'namespace',
                    rule_signature TEXT,
                    intent_sig TEXT,
                    intent_sha TEXT
                )
                """
            )
        )
        cx.execute(
            text(
                """
                INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled, rule_signature, intent_sha)
                VALUES (:q, :kind, :payload, 1, :sig, :sha)
                """
            ),
            {
                "q": "list contracts where department = support services",
                "kind": "eq",
                "payload": json.dumps({"eq_filters": [["DEPARTMENT", ["SUPPORT SERVICES"]]]}),
                "sig": default_sig,
                "sha": default_sha256,
            },
        )

    _enable_knobs(monkeypatch, fts_shape="groups_only", eq_list_mode="any_len")

    question_intent = {
        "eq_filters": [["DEPARTMENT", ["ARCHITECTURE & DESIGN"]]],
        "fts_groups": [["rent contract"], ["maintenance"]],
    }

    merged = load_rules_for_question(engine, "list contracts where department = support services", question_intent)

    assert merged.get("eq_filters"), "expected legacy rule to apply despite knob changes"
