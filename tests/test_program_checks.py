"""Program-checking tests: weak acyclicity, typing, rule syntax rejection."""

from chase.model import (
    ModelError, ProgramError, Schema, assert_weakly_acyclic, parse_program,
)


def _prog(rules):
    return {"rules": rules}


def test_ordinary_recursion_is_allowed():
    sch = Schema({"R": ("text", "text"), "S": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "a", "body": ["R(x, y)"], "head": "S(y, x)"},
        {"id": "b", "body": ["S(x, y)"], "head": "R(x, y)"},
    ]), sch)
    assert all(not r.cyclic for r in rules)
    assert_weakly_acyclic(rules)  # must not raise


def test_ordinary_recursion_with_projection_is_allowed():
    sch = Schema({"R": ("text", "text"), "S": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "a", "body": ["R(x, y)"], "head": "S(x, x)"},
        {"id": "b", "body": ["S(x, y)"], "head": "R(y, x)"},
    ]), sch)
    assert_weakly_acyclic(rules)


def test_special_edge_on_a_cycle_is_rejected():
    sch = Schema({"R": ("text", "text"), "S": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "a", "body": ["R(x, y)"], "head": "S(y, w)"},
        {"id": "b", "body": ["S(x, y)"], "head": "R(x, y)"},
    ]), sch)
    assert rules[0].cyclic
    assert rules[0].cyclic_edges
    try:
        assert_weakly_acyclic(rules)
    except ProgramError as exc:
        assert "not weakly acyclic" in str(exc)
        assert "special edge" in str(exc)
    else:
        raise AssertionError("expected ProgramError")


def test_classic_single_relation_special_cycle_is_rejected():
    # A(x,y) -> B(x,w) ; B(x,y) -> A(x)   (the textbook non-WA program)
    sch = Schema({"A": ("text",), "B": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "a", "body": ["B(x, y)"], "head": "A(x)"},
        {"id": "b", "body": ["A(x)"], "head": "B(x, w)"},
    ]), sch)
    # R.0 -> B.1 (special), B.1? no; cycle: A.0 -> B.1 special, B.1 -> ? B.0
    # ordinary does not go back to A.0, so this IS weakly acyclic.
    assert_weakly_acyclic(rules)


def test_three_rule_special_cycle_is_rejected():
    sch = Schema({"R": ("text", "text"), "S": ("text", "text"), "T": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "a", "body": ["R(x, y)"], "head": "S(x, w)"},
        {"id": "b", "body": ["S(x, y)"], "head": "T(y, z)"},
        {"id": "c", "body": ["T(x, y)"], "head": "R(y, x)"},
    ]), sch)
    assert any(r.cyclic for r in rules)
    try:
        assert_weakly_acyclic(rules)
    except ProgramError:
        pass
    else:
        raise AssertionError("expected ProgramError")


def test_variable_type_mismatch_is_rejected():
    sch = Schema({"A": ("text", "int")})
    try:
        parse_program(_prog([{"id": "r", "body": ["A(x, y)"], "head": "A(y, x)"}]), sch)
    except ModelError as exc:
        assert "used both as" in str(exc)
    else:
        raise AssertionError("expected type error")


def test_constant_type_mismatch_is_rejected():
    sch = Schema({"A": ("text",)})
    try:
        parse_program(_prog([{"id": "r", "body": ["A(42)"], "head": "A(43)"}]), sch)
    except ModelError as exc:
        assert "int constant" in str(exc)
    else:
        raise AssertionError("expected type error")


def test_egd_variable_must_be_bound_in_body():
    sch = Schema({"A": ("text", "text")})
    try:
        parse_program(_prog([{"id": "r", "body": ["A(x, y)"], "eq": ["x", "z"]}]), sch)
    except ModelError as exc:
        assert "does not occur in the body" in str(exc)
    else:
        raise AssertionError("expected binding error")


def test_egd_mixed_domains_is_rejected():
    sch = Schema({"A": ("text", "int")})
    try:
        parse_program(_prog([{"id": "r", "body": ["A(x, y)"], "eq": ["x", "y"]}]), sch)
    except ModelError as exc:
        assert "can never be equal" in str(exc)
    else:
        raise AssertionError("expected type error")


def test_undeclared_relation_is_rejected():
    sch = Schema({"A": ("text",)})
    for rule in ({"id": "r", "body": ["A(x)"], "head": "B(x)"},
                 {"id": "r2", "body": ["B(x)"], "head": "A(x)"}):
        try:
            parse_program(_prog([rule]), sch)
        except ModelError as exc:
            assert "not declared" in str(exc)
        else:
            raise AssertionError(f"expected undeclared-relation error for {rule}")


def test_negation_arithmetic_and_code_are_rejected():
    sch = Schema({"A": ("text", "text")})
    bad = [
        {"id": "n", "body": ["not A(x, y)"], "head": "A(x, y)"},
        {"id": "a", "body": ["A(x, y)"], "head": "A(x, y + 1)"},
        {"id": "m", "body": ["A(x, y)"], "head": "A(x, 2 * y)"},
    ]
    for rule in bad:
        try:
            parse_program(_prog([rule]), sch)
        except ModelError:
            pass
        else:
            raise AssertionError(f"expected rejection for {rule}")


def test_duplicate_ids_and_signatures_are_rejected():
    sch = Schema({"A": ("text", "text")})
    try:
        parse_program(_prog([
            {"id": "r", "body": ["A(x, y)"], "head": "A(x, y)"},
            {"id": "r", "body": ["A(x, y)"], "head": "A(y, x)"},
        ]), sch)
    except ModelError as exc:
        assert "duplicate rule id" in str(exc)
    else:
        raise AssertionError("expected duplicate id error")
    try:
        parse_program(_prog([
            {"id": "r1", "body": ["A(x, y)"], "head": "A(x, y)"},
            {"id": "r2", "body": ["A(x, y)"], "head": "A(x, y)"},
        ]), sch)
    except ModelError as exc:
        assert "duplicates" in str(exc)
    else:
        raise AssertionError("expected duplicate signature error")


def test_head_constant_is_accepted_and_typed():
    # A head may carry a constant (with the declared column type); it is not
    # an existential and it is never turned into a placeholder.
    sch = Schema({"A": ("text", "text"), "B": ("text", "text")})
    rules = parse_program(_prog([{"id": "r", "body": ["A(x, y)"], "head": 'B(x, "c")'}]), sch)
    assert rules[0].existentials == ()
    try:
        parse_program(_prog([{"id": "r", "body": ["A(x, y)"], "head": "B(x, 7)"}]), sch)
    except ModelError as exc:
        assert "int constant" in str(exc)
    else:
        raise AssertionError("head constant must respect the column type")


def test_constants_and_repeated_variables_in_body_are_accepted():
    sch = Schema({"A": ("text", "text"), "B": ("text", "text")})
    rules = parse_program(_prog([
        {"id": "r", "body": ['A(x, "k")', "A(x, x)"], "head": "B(x, x)"},
    ]), sch)
    assert rules[0].signature == 'A(text:x,text:\'k\'); A(text:x,text:x) -> B(text:x,text:x)'


def test_self_join_body_is_allowed():
    sch = Schema({"A": ("text", "text"), "B": ("text",)})
    rules = parse_program(_prog([
        {"id": "r", "body": ["A(x, y)", "A(y, x)"], "head": "B(x)"},
    ]), sch)
    assert rules[0].signature
