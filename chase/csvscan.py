"""Streaming, strict CSV scanner.

The snapshot feed is fixed-format CSV: UTF-8 (an optional leading BOM is
accepted exactly once), comma delimiters, double-quote escaping (a literal
quote inside a quoted field is written ``""``), CRLF *or* LF line endings,
and the final record need not end with a newline.  Quoted fields may contain
commas, CR/LF and quotes, so records **cannot** be found by splitting on
physical lines.

This module implements its own character-level state machine instead of
relying on :mod:`csv` because the ingestion layer must:

* stream records one at a time (a whole snapshot never fits in memory by
  design — see :mod:`chase.ingest`);
* keep, for every logical record, its logical sequence number **and** the
  physical line span it occupies, so an integrator can jump back to the exact
  bad line in the supplier's file;
* turn every structural defect (broken quote, data after a closing quote,
  bad UTF-8, bare CR, ...) into an explicit, located error rather than
  guessing a repair.

The scanner is a pure byte/character machine: it performs no type coercion
and knows nothing about headers or schemas.  Everything field-related
(duplicate headers, missing mapped columns, field-count mismatches) is
decided by the ingestion layer on top of the logical records delivered here.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from typing import BinaryIO, Iterator, List, Optional

# scanner states
S_FIELD_START = 0   # no character of this field seen yet
S_UNQUOTED = 1      # inside an unquoted field
S_QUOTED = 2        # inside a quoted field
S_QUOTE_PENDING = 3 # one closing quote seen inside a quoted field ("")
S_CR_PENDING = 4    # CR seen outside quotes; the next char must be LF


class CSVStructureError(Exception):
    """A structural defect in the CSV byte stream.

    Never a content/type error: this is purely about quotes, delimiters,
    line endings and byte-level encoding.
    """

    def __init__(self, code: str, message: str, physical_line: int,
                 byte_offset: int) -> None:
        self.code = code
        self.physical_line = physical_line
        self.byte_offset = byte_offset
        super().__init__(f"{code} at physical line {physical_line} "
                         f"(byte {byte_offset}): {message}")


@dataclass(frozen=True)
class CSVRecord:
    """One logical CSV record (the header is logical record sequence 1)."""

    sequence: int            # 1-based logical record number within the file
    physical_start: int      # physical line of the record's first character
    physical_end: int        # physical line of its terminating newline (==
                             # start for a single-line record); at EOF without
                             # a trailing newline this is the last line
    fields: List[str]        # field text exactly as quoted (quotes unescaped)
    byte_start: int          # byte offset of the record's first character
    byte_end: int            # byte offset just past the record terminator


def iter_records(fh: BinaryIO, source_name: str = "<csv>"
                 ) -> Iterator[CSVRecord]:
    """Yield logical CSV records from an open binary file.

    Raises :class:`CSVStructureError` on the first structural defect.  The
    exception carries the physical line and byte offset so callers can point
    at the supplier's exact bad line.  Only one record at a time is held in
    scanner state; callers decide what to persist.
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    state = S_FIELD_START
    field_buf: List[str] = []
    record: List[str] = []
    sequence = 0
    physical_line = 1                 # physical line of the next/current char
    record_start_line: Optional[int] = None
    record_start_byte = 0
    byte_offset = 0
    bom_checked = False
    have_record_content = False       # any char of the current record seen

    def fail(code: str, message: str) -> CSVStructureError:
        return CSVStructureError(code, f"{source_name}: {message}",
                                 physical_line, byte_offset)

    def begin_record_if_needed() -> None:
        nonlocal record_start_line, record_start_byte
        if record_start_line is None:
            record_start_line = physical_line
            record_start_byte = byte_offset

    def close_record(end_line: int, end_byte: int) -> CSVRecord:
        record.append("".join(field_buf))
        return CSVRecord(
            sequence=sequence,
            physical_start=record_start_line,  # type: ignore[arg-type]
            physical_end=end_line,
            fields=record,
            byte_start=record_start_byte,
            byte_end=end_byte,
        )

    def feed(ch: str) -> Iterator[CSVRecord]:
        nonlocal state, field_buf, record, sequence, physical_line
        nonlocal byte_offset, record_start_line, have_record_content

        begin_record_if_needed()

        # ---- line endings ------------------------------------------------ #
        if ch == "\r":
            byte_offset += 1
            if state == S_QUOTED:
                field_buf.append(ch)               # CR inside quotes: content
                return
            state = S_CR_PENDING
            return
        if ch == "\n":
            if state == S_CR_PENDING:
                # accepted CRLF terminator
                byte_offset += 1
                sequence += 1
                yield close_record(physical_line, byte_offset)
                physical_line += 1
                state = S_FIELD_START
                field_buf = []
                record = []
                record_start_line = None
                have_record_content = False
                return
            byte_offset += 1
            if state == S_QUOTED:
                physical_line += 1                # a physical line even inside
                field_buf.append(ch)              # quotes; LF is still content
                return
            sequence += 1
            yield close_record(physical_line, byte_offset)
            physical_line += 1
            state = S_FIELD_START
            field_buf = []
            record = []
            record_start_line = None
            have_record_content = False
            return
        if state == S_CR_PENDING:
            raise fail("bare-cr",
                       "bare carriage return outside quotes; only CRLF and "
                       "LF line endings are accepted")

        byte_offset += 1
        have_record_content = True

        # ---- commas ------------------------------------------------------ #
        if ch == ",":
            if state == S_QUOTED:
                field_buf.append(ch)
                return
            if state == S_QUOTE_PENDING:
                record.append("".join(field_buf))  # quoted field closed
                field_buf = []
                state = S_FIELD_START
                return
            record.append("".join(field_buf))
            field_buf = []
            state = S_FIELD_START
            return

        # ---- quotes ------------------------------------------------------ #
        if ch == '"':
            if state == S_FIELD_START:
                state = S_QUOTED
                return
            if state == S_QUOTED:
                state = S_QUOTE_PENDING
                return
            if state == S_QUOTE_PENDING:
                field_buf.append('"')              # escaped "" -> literal "
                state = S_QUOTED
                return
            raise fail("malformed-quote",
                       "a double quote may only start a quoted field or be "
                       "doubled (\"\") inside one")

        # ---- any other character ----------------------------------------- #
        if state == S_QUOTE_PENDING:
            raise fail("text-after-quote",
                       "only a doubled quote, a comma, a line ending or EOF "
                       "may follow a closing quote")
        if state == S_FIELD_START:
            state = S_UNQUOTED
        field_buf.append(ch)

    while True:
        chunk = fh.read(1 << 16)
        if not chunk:
            break
        try:
            text = decoder.decode(chunk, final=False)
        except UnicodeDecodeError as exc:
            raise fail("bad-utf8",
                       f"invalid UTF-8 byte sequence: {exc.reason}") from None
        if not bom_checked:
            bom_checked = True
            if text.startswith("﻿"):
                text = text[1:]
                byte_offset += 3
        for ch in text:
            for rec in feed(ch):
                yield rec

    try:
        tail = decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise fail("bad-utf8",
                   f"invalid UTF-8 byte sequence at EOF: {exc.reason}") from None
    for ch in tail:
        for rec in feed(ch):
            yield rec

    # ---- EOF -------------------------------------------------------------- #
    if state == S_CR_PENDING:
        raise fail("bare-cr",
                   "file ends with a bare carriage return outside quotes")
    if state == S_QUOTED:
        raise fail("unterminated-quote",
                   "a quoted field is still open at end of file")
    if state == S_QUOTE_PENDING:
        # the last character was a valid closing quote: accept the record
        state = S_FIELD_START
    if have_record_content or record or field_buf or record_start_line is not None:
        sequence += 1
        yield close_record(physical_line, byte_offset)
