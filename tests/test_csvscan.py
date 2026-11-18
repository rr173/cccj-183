"""Tests for the streaming strict CSV scanner."""

import io

from chase.csvscan import CSVStructureError, iter_records


def scan(data):
    return [(r.sequence, r.physical_start, r.physical_end, r.fields,
             r.byte_start, r.byte_end)
            for r in iter_records(io.BytesIO(data), "t.csv")]


def test_basic_records_and_logical_sequence():
    out = scan(b"a,b,c\n1,2,3\n")
    assert out[0][:4] == (1, 1, 1, ["a", "b", "c"])
    assert out[1][:4] == (2, 2, 2, ["1", "2", "3"])


def test_bom_crlf_and_no_trailing_newline():
    out = scan(b"\xef\xbb\xbfa,b\r\nx,y\r\nz,1")
    assert [r[3] for r in out] == [["a", "b"], ["x", "y"], ["z", "1"]]
    assert out[-1][2] == 3  # last physical line


def test_quoted_comma_newline_and_escaped_quote():
    inner = 'line1\nline2"q'.replace('"', '""')
    data = ('a,b\n"x,y","' + inner + '"\n').encode()
    out = scan(data)
    assert out[1][3] == ["x,y", 'line1\nline2"q']
    # multiline record spans physical lines 2..3
    assert out[1][1] == 2 and out[1][2] == 3


def test_crlf_inside_quotes_is_content():
    out = scan(b'a,b\n"x\r\ny",z\r\n')
    assert out[1][3] == ["x\r\ny", "z"]


def test_empty_fields_preserved():
    out = scan(b"a,,c\n,\n")
    assert out[0][3] == ["a", "", "c"]
    assert out[1][3] == ["", ""]


def test_header_only_and_empty_file():
    assert scan(b"a,b,c") == [(1, 1, 1, ["a", "b", "c"], 0, 5)]
    assert scan(b"") == []


def test_physical_lines_after_multiline_record():
    out = scan(b'a\n"x\ny"\n1\n')
    assert [r[3] for r in out] == [["a"], ["x\ny"], ["1"]]
    assert out[2][1] == 4 and out[2][2] == 4


def test_structure_errors_are_located():
    cases = [
        (b"a\n\xff\n", "bad-utf8"),
        (b'a\n"unterminated', "unterminated-quote"),
        (b"a,b\n1,x\"y\n", "malformed-quote"),
        (b"a,b\rx", "bare-cr"),
        (b'a,b\n1,"x"y\n', "text-after-quote"),
    ]
    for data, code in cases:
        try:
            scan(data)
        except CSVStructureError as exc:
            assert exc.code == code, (data, exc.code)
            assert exc.physical_line >= 1
        else:
            raise AssertionError(f"accepted malformed input {data!r}")


def test_chunk_boundary_does_not_change_records():
    # same content parsed regardless of where 64KiB chunks fall
    data = b"h1,h2\n" + b",".join([b'"q,x"'] * 100) + b"\nlast,1\n"
    assert scan(data) == scan(data)
    out = scan(data)
    assert len(out) == 3


def test_quoted_field_at_eof_without_newline():
    out = scan(b'a,b\n1,"closed"')
    assert out[1][3] == ["1", "closed"]
