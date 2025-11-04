import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.learning_store import _canon_signature_from_intent  # noqa: E402


def _build_intent(
    *,
    fts_groups=None,
    fts_tokens=None,
    stakeholder_vals=None,
    department_val=None,
    operator="OR",
):
    intent = {
        "eq_filters": [
            ["STAKEHOLDER", stakeholder_vals or ["A", "B"]],
            ["DEPARTMENTS", [department_val or "SUPPORT SERVICES"]],
        ],
        "fts_groups": fts_groups,
        "fts_tokens": fts_tokens,
        "fts_operator": operator,
        "order": {"col": "REQUEST_DATE", "desc": True},
    }
    return intent


def test_signature_ignores_eq_value_variations():
    base_intent = _build_intent(
        stakeholder_vals=["Tamer Said Aly Abdelgawad", "u1835"],
        department_val="SUPPORT SERVICES",
        fts_groups=[["home care"], ["maintenance"]],
    )
    variant_intent = _build_intent(
        stakeholder_vals=["tamer said aly abdelgawad", "U1835"],
        department_val="support service",
        fts_groups=[["home cares"], ["maintenances"]],
    )

    sig1 = _canon_signature_from_intent(base_intent)[2]
    sig2 = _canon_signature_from_intent(variant_intent)[2]

    assert sig1 == sig2


def test_signature_ignores_fts_token_text_changes():
    intent_a = _build_intent(fts_groups=[["ppm for 3 chillers in dsfmc"], ["solar clinic"]])
    intent_b = _build_intent(
        fts_tokens=["PPM FOR 3 CHILLERS IN DSFMC", "solar clinic"],
        stakeholder_vals=["Tamer", "u9981"],
    )

    sig_a = _canon_signature_from_intent(intent_a)[2]
    sig_b = _canon_signature_from_intent(intent_b)[2]

    assert sig_a == sig_b
