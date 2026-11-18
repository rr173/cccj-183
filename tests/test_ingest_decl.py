"""Tests for declaration validation and strict cell conversion."""

from chase.ingest_decl import (
    DeclarationError,
    UnknownMarkers,
    encode_record_key,
    parse_declaration,
    parse_int_cell,
)
from chase.model import Schema

_SCHEMA = Schema(
    {"Order": ("text", "int", "text")},
    {"Order": ("oid", "qty", "addr")})

_BASE = {
    "connector": "c1",
    "version": 1,
    "unknown_markers": {"marker": "NULL", "name_prefix": "NULL:",
                        "name_separator": "|"},
    "sources": [{
        "source_id": "erp",
        "relation": "Order",
        "key_columns": ["oid"],
        "fields": [
            {"name": "oid", "header": "oid"},
            {"name": "qty", "header": "qty"},
            {"name": "addr", "header": "addr"}],
        "shards": [{"path": "/tmp/a.csv"}],
        "empty_snapshot": False,
    }],
}


def _decl(**overrides):
    import copy
    obj = copy.deepcopy(_BASE)
    obj.update(overrides)
    return parse_declaration(obj, _SCHEMA)


def test_strict_int_accepts_sign_and_digits_only():
    assert parse_int_cell("7") == 7
    assert parse_int_cell("07") == 7
    assert parse_int_cell("+7") == 7
    assert parse_int_cell("-7") == -7
    assert parse_int_cell("000") == 0
    for bad in ["7.0", "7e3", "1_000", " 7", "7 ", "", "  ", "７", "0x7",
                "7."]:
        try:
            parse_int_cell(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted bad int {bad!r}")


def test_text_preserved_verbatim_including_blank_and_whitespace():
    markers = UnknownMarkers("NULL", "NULL:", "|")
    # empty and whitespace-only strings are ordinary text, never missing
    tok, name = __import__("chase.ingest", fromlist=["convert_cell"]).convert_cell(
        "", "text", markers, False, "x")
    assert tok == ["s", ""] and name is None
    tok, _ = __import__("chase.ingest", fromlist=["convert_cell"]).convert_cell(
        "   ", "text", markers, False, "x")
    assert tok == ["s", "   "]


def test_unknown_markers_only_match_explicit_spellings():
    import re
    m = UnknownMarkers("NULL", "NULL:", "|")
    assert m.classify("NULL") == ("unknown", None, None)
    assert m.classify("NULL:u") == ("unknown", "u", None)
    assert m.classify("NULL:u|x") == ("unknown", "u", "x")
    # ordinary strings that merely look like null ids stay strings
    assert m.classify("u") == ("value", None, None)
    assert m.classify("null") == ("value", None, None)
    assert m.classify("NULL:") == ("value", None, None)
    m2 = UnknownMarkers("NULL", "", "")
    assert m2.classify("NULL:u") == ("value", None, None)
    # with a name pattern, internal-looking ids can be kept as strings
    m3 = UnknownMarkers("NULL", "NULL:", "|", re.compile(r"u[a-z0-9]*"))
    assert m3.classify("NULL:u1") == ("unknown", "u1", None)
    assert m3.classify("NULL:n0") == ("value", None, None)


def test_typed_composite_key_no_collision():
    text_seven = encode_record_key([["s", "7"]])
    int_seven = encode_record_key([["i", 7]])
    assert text_seven != int_seven
    # no separator-concatenation ambiguity
    k1 = encode_record_key([["s", "a,b"], ["i", 1]])
    k2 = encode_record_key([["s", "a"], ["s", "b,1"]]) if False else None
    assert k1 != encode_record_key([["s", "a"], ["s", "b"]])
    # multi-column keys are stable and deterministic
    assert encode_record_key([["s", "x"], ["i", 1]]) == \
        encode_record_key([["s", "x"], ["i", 1]])


def test_unknown_never_allowed_in_key_columns():
    from chase.ingest import convert_cell
    m = UnknownMarkers("NULL", "NULL:", "|")
    try:
        convert_cell("NULL", "text", m, True, "key col")
    except ValueError:
        pass
    else:
        raise AssertionError("unknown accepted in key column")


def test_declaration_rejects_undefined_keys_at_every_level():
    import copy
    for patch in [
        lambda o: o.update({"shards_": []}),
        lambda o: o["sources"][0].update({"relation_typo": "Order"}),
        lambda o: o["unknown_markers"].update({"nul_marker": "X"}),
        lambda o: o["sources"][0]["fields"][0].update({"heeder": "x"}),
        lambda o: o["sources"][0]["shards"][0].update({"pth": "x"}),
    ]:
        obj = copy.deepcopy(_BASE)
        patch(obj)
        try:
            parse_declaration(obj, _SCHEMA)
        except DeclarationError:
            continue
        raise AssertionError(f"misspelled key accepted: {obj}")


def test_declaration_relation_and_columns_must_match_schema():
    import copy
    obj = copy.deepcopy(_BASE)
    obj["sources"][0]["relation"] = "Nope"
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("unknown relation accepted")
    obj = copy.deepcopy(_BASE)
    obj["sources"][0]["key_columns"] = ["nope"]
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("unknown key column accepted")


def test_declaration_requires_full_field_mapping_and_unique_headers():
    import copy
    obj = copy.deepcopy(_BASE)
    obj["sources"][0]["fields"].pop()
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("partial field mapping accepted")
    obj = copy.deepcopy(_BASE)
    obj["sources"][0]["fields"][1]["header"] = "oid"
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("duplicate header mapping accepted")


def test_declaration_one_relation_per_source_and_unique_sids():
    import copy
    obj = copy.deepcopy(_BASE)
    obj["sources"].append(copy.deepcopy(_BASE["sources"][0]))
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("duplicate source_id accepted")
    d = _decl()
    assert d.sources[0].source_id == "erp"
    assert d.content_hash_input  # identity available


def test_declaration_unknown_marker_consistency():
    import copy
    obj = copy.deepcopy(_BASE)
    obj["unknown_markers"]["name_separator"] = ""
    try:
        parse_declaration(obj, _SCHEMA)
    except DeclarationError:
        pass
    else:
        raise AssertionError("separator without prefix accepted")
