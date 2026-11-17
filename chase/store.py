"""SQLite-backed fact storage.

Facts live in one table per relation, all values interned in a shared value
table, so equal values (including a merged unknown and a constant) are a
single identity at the storage layer too.  The store knows nothing about
rules: canonicalization happens before rows reach it.  A rebuild rewrites the
affected relations through a translator, which is how EGD unification removes
duplicate rows and re-points every old/new reference to the class
representative.

Each relation table has an extra ``src`` column: 1 marks a fact that was
present in the input, 0 a fact derived by a rule.  Merging two rows keeps the
OR of the flags, so an input fact that unifies with a derived row is still
reported as input-sourced.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .model import BudgetExceeded, ModelError, value_key

Ks = Tuple[object, object]  # ("s", str) | ("i", int) | ("n", str)


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class Store:
    """Disk-backed set-of-facts store.

    ``path`` can be a file path or ``":memory:"``.  Relations are declared
    once and get a ``WITHOUT ROWID`` table with a primary key over all
    columns plus the ``src`` flag.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            if parent:
                os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.isolation_level = None  # explicit BEGIN for bulk loads
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA cache_size=-131072")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS _val ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " kind TEXT NOT NULL, val TEXT NOT NULL,"
            " UNIQUE(kind, val))")
        self._uid_cache: Dict[Ks, int] = {}
        self._ks_cache: Dict[int, Ks] = {}
        self.relations: Dict[str, Tuple[str, ...]] = {}
        self.indexes: Dict[str, List[Tuple[str, Tuple[int, ...]]]] = {}
        self._in_tx = False

    # -- transactions ------------------------------------------------------ #
    def begin(self) -> None:
        if not self._in_tx:
            self.conn.execute("BEGIN")
            self._in_tx = True

    def commit(self) -> None:
        if self._in_tx:
            self.conn.execute("COMMIT")
            self._in_tx = False

    def rollback(self) -> None:
        if self._in_tx:
            try:
                self.conn.execute("ROLLBACK")
            finally:
                self._in_tx = False

    # -- value interning ---------------------------------------------------- #
    @staticmethod
    def ks_for(value: object) -> Ks:
        return value_key(value)  # type: ignore[return-value]

    def uid(self, key: Ks) -> int:
        cached = self._uid_cache.get(key)
        if cached is not None:
            return cached
        kind, val = key
        text = str(val)
        self.conn.execute("INSERT OR IGNORE INTO _val(kind, val) VALUES (?, ?)", (kind, text))
        row = self.conn.execute("SELECT id FROM _val WHERE kind=? AND val=?",
                                (kind, text)).fetchone()
        assert row is not None
        uid = int(row[0])
        self._uid_cache[key] = uid
        self._ks_cache[uid] = key
        return uid

    def ks(self, uid: int) -> Ks:
        hit = self._ks_cache.get(uid)
        if hit is not None:
            return hit
        row = self.conn.execute("SELECT kind, val FROM _val WHERE id=?", (uid,)).fetchone()
        if row is None:
            raise ModelError(f"corrupt value store: unknown uid {uid}")
        kind, text = row
        key: Ks = (kind, text if kind in ("s", "n") else int(text))
        self._ks_cache[uid] = key
        return key

    # -- relations ---------------------------------------------------------- #
    def declare(self, rel: str, arity: int) -> None:
        if rel in self.relations:
            return
        cols = ", ".join(f"c{i} INTEGER NOT NULL" for i in range(arity))
        pk = ", ".join(f"c{i}" for i in range(arity))
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {_quote('f_' + rel)} "
            f"({cols}, src INTEGER NOT NULL DEFAULT 0, PRIMARY KEY ({pk})) WITHOUT ROWID")
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {_quote('ix_' + rel + '_src')} "
            f"ON {_quote('f_' + rel)} (src)")
        self.relations[rel] = tuple(f"c{i}" for i in range(arity))
        self.indexes[rel] = []
        # single column indexes let the matcher use any bound position
        for i in range(arity):
            self.ensure_index(rel, f"ix_{rel}_c{i}", (i,))

    def ensure_index(self, rel: str, name: str, cols: Sequence[int]) -> bool:
        for existing, _ in self.indexes.get(rel, ()):
            if existing == name:
                return False
        idx_cols = ", ".join(f"c{i}" for i in cols)
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {_quote(name)} ON {_quote('f_' + rel)} ({idx_cols})")
        self.indexes.setdefault(rel, []).append((name, tuple(cols)))
        return True

    # -- writes ------------------------------------------------------------- #
    def insert(self, rel: str, values: Sequence[Ks], src: int = 0) -> bool:
        if len(values) != len(self.relations[rel]):
            raise ModelError(f"arity mismatch inserting into {rel}")
        uids = [self.uid(v) for v in values]
        marks = ", ".join("?" for _ in uids)
        cur = self.conn.execute(
            f"INSERT OR IGNORE INTO {_quote('f_' + rel)} VALUES ({marks}, ?)",
            uids + [1 if src else 0])
        inserted = cur.rowcount > 0
        if not inserted and src:
            self.conn.execute(
                f"UPDATE {_quote('f_' + rel)} SET src=1 WHERE "
                + " AND ".join(f"c{i}=?" for i in range(len(uids))), uids)
        return inserted

    def insert_many(self, rel: str, rows: Iterable[Sequence[Ks]], src: int = 0) -> int:
        arity = len(self.relations[rel])
        marks = ", ".join("?" for _ in range(arity))
        flag = 1 if src else 0
        sql = f"INSERT OR IGNORE INTO {_quote('f_' + rel)} VALUES ({marks}, ?)"
        batch: List[Tuple[int, ...]] = []
        added = 0
        for values in rows:
            if len(values) != arity:
                raise ModelError(f"arity mismatch inserting into {rel}")
            batch.append(tuple(self.uid(v) for v in values) + (flag,))
            if len(batch) >= 2000:
                before = self.conn.total_changes
                self.conn.executemany(sql, batch)
                added += self.conn.total_changes - before
                batch = []
        if batch:
            before = self.conn.total_changes
            self.conn.executemany(sql, batch)
            added += self.conn.total_changes - before
        return added

    # -- reads -------------------------------------------------------------- #
    def _where(self, fixed: Sequence[Optional[Ks]]) -> Tuple[str, List[int]]:
        conds: List[str] = []
        params: List[int] = []
        for i, v in enumerate(fixed):
            if v is not None:
                conds.append(f"c{i}=?")
                params.append(self.uid(v))
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        return where, params

    def _group_sql(self, groups: Iterable[Sequence[int]]) -> str:
        extra: List[str] = []
        for group in groups:
            for j in group[1:]:
                extra.append(f"c{j}=c{group[0]}")
        return " AND ".join(extra)

    def fetch_matching(self, rel: str, fixed: Sequence[Optional[Ks]],
                       groups: Iterable[Sequence[int]] = (), cap: Optional[int] = None
                       ) -> List[Tuple[Ks, ...]]:
        """All rows matching fixed positions and equality groups.

        Raises :class:`~chase.model.BudgetExceeded` when ``cap`` is exceeded:
        the caller asked for a bounded materialization, not a full join.
        """
        where, params = self._where(fixed)
        extra = self._group_sql(groups)
        if extra:
            where += (" AND " if where else " WHERE ") + extra
        sql = f"SELECT * FROM {_quote('f_' + rel)}{where}"
        if cap is not None:
            rows = self.conn.execute(sql + " LIMIT ?", params + [cap + 1]).fetchall()
            if len(rows) > cap:
                raise BudgetExceeded(
                    "match-rows",
                    f"relation {rel} has more than {cap} matching rows for one body atom; "
                    f"bound the input or raise max_match_rows")
        else:
            rows = self.conn.execute(sql, params).fetchall()
        return [tuple(self.ks(int(u)) for u in row[:-1]) for row in rows]

    def relation_has_any(self, rel: str, values: Sequence[Ks]) -> bool:
        if not values:
            return False
        arity = len(self.relations[rel])
        # chunk the OR-list: SQLite rejects very deep expression trees
        batch = max(1, 500 // max(1, arity))
        for start in range(0, len(values), batch):
            chunk = values[start:start + batch]
            clauses: List[str] = []
            params: List[int] = []
            for i in range(arity):
                for v in chunk:
                    clauses.append(f"c{i}=?")
                    params.append(self.uid(v))
            sql = (f"SELECT 1 FROM {_quote('f_' + rel)} WHERE "
                   f"{' OR '.join(clauses)} LIMIT 1")
            if self.conn.execute(sql, params).fetchone() is not None:
                return True
        return False

    def rows_containing(self, rel: str, value: Ks, chunk: int = 256
                        ) -> Iterator[Tuple[Tuple[Ks, ...], int]]:
        arity = len(self.relations[rel])
        conds = " OR ".join(f"c{i}=?" for i in range(arity))
        params = [self.uid(value)] * arity
        cur = self.conn.execute(
            f"SELECT * FROM {_quote('f_' + rel)} WHERE {conds}", params)
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            for row in rows:
                yield tuple(self.ks(int(u)) for u in row[:-1]), int(row[-1])

    def has(self, rel: str, values: Sequence[Ks]) -> bool:
        uids = [self.uid(v) for v in values]
        where = " AND ".join(f"c{i}=?" for i in range(len(uids)))
        row = self.conn.execute(
            f"SELECT 1 FROM {_quote('f_' + rel)} WHERE {where} LIMIT 1", uids).fetchone()
        return row is not None

    def match(self, rel: str, fixed: Sequence[Optional[Ks]]) -> Optional[Tuple[Ks, ...]]:
        where, params = self._where(fixed)
        row = self.conn.execute(
            f"SELECT * FROM {_quote('f_' + rel)}{where} LIMIT 1", params).fetchone()
        if row is None:
            return None
        return tuple(self.ks(int(u)) for u in row[:-1])

    def exists(self, rel: str, fixed: Sequence[Optional[Ks]],
               equal_groups: Iterable[Sequence[int]] = ()) -> bool:
        where, params = self._where(fixed)
        extra: List[str] = []
        for group in equal_groups:
            for j in group[1:]:
                extra.append(f"c{j}=c{group[0]}")
        if extra:
            where += (" AND " if where else " WHERE ") + " AND ".join(extra)
        row = self.conn.execute(
            f"SELECT 1 FROM {_quote('f_' + rel)}{where} LIMIT 1", params).fetchone()
        return row is not None

    def iter_matching(self, rel: str, fixed: Sequence[Optional[Ks]],
                      groups: Iterable[Sequence[int]] = (),
                      chunk: int = 512) -> Iterator[Tuple[Tuple[Ks, ...], int]]:
        where, params = self._where(fixed)
        extra: List[str] = []
        for group in groups:
            for j in group[1:]:
                extra.append(f"c{j}=c{group[0]}")
        if extra:
            where += (" AND " if where else " WHERE ") + " AND ".join(extra)
        cur = self.conn.execute(f"SELECT * FROM {_quote('f_' + rel)}{where}", params)
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            for row in rows:
                yield tuple(self.ks(int(u)) for u in row[:-1]), int(row[-1])

    def count(self, rel: str) -> int:
        row = self.conn.execute(f"SELECT COUNT(*) FROM {_quote('f_' + rel)}").fetchone()
        return int(row[0])

    def iter_rows(self, rel: str, chunk: int = 1000) -> Iterator[Tuple[Tuple[Ks, ...], int]]:
        cur = self.conn.execute(f"SELECT * FROM {_quote('f_' + rel)}")
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            for row in rows:
                yield tuple(self.ks(int(u)) for u in row[:-1]), int(row[-1])

    def rows(self, rel: str) -> List[Tuple[Tuple[Ks, ...], int]]:
        return list(self.iter_rows(rel))

    def all_counts(self) -> Dict[str, int]:
        return {rel: self.count(rel) for rel in self.relations}

    def total_facts(self) -> int:
        return sum(self.count(rel) for rel in self.relations)

    # -- unification rewrite ------------------------------------------------ #
    def rebuild(self, rels: Iterable[str],
                translate: Callable[[Tuple[Ks, ...]], Tuple[Ks, ...]]
                ) -> Dict[str, Dict[str, int]]:
        """Rewrite the given relations through ``translate``.

        Every physical row is re-canonicalized and re-inserted into a fresh
        table; rows that collapse onto an existing tuple are dropped and their
        ``src`` flags OR-ed.  Returns per-relation counts.
        """
        self.begin()
        stats: Dict[str, Dict[str, int]] = {}
        try:
            for rel in rels:
                if rel not in self.relations:
                    continue
                arity = len(self.relations[rel])
                before = self.count(rel)
                tmp = f"_tmp_{rel}"
                cols = ", ".join(f"c{i} INTEGER NOT NULL" for i in range(arity))
                pk = ", ".join(f"c{i}" for i in range(arity))
                self.conn.execute(f"DROP TABLE IF EXISTS {_quote(tmp)}")
                self.conn.execute(
                    f"CREATE TABLE {_quote(tmp)} "
                    f"({cols}, src INTEGER NOT NULL DEFAULT 0, PRIMARY KEY ({pk})) WITHOUT ROWID")
                marks = ", ".join("?" for _ in range(arity))
                sql = (f"INSERT INTO {_quote(tmp)} VALUES ({marks}, ?) "
                       f"ON CONFLICT DO UPDATE SET src=MAX(src, excluded.src)")
                batch = []
                for row, src in self.iter_rows(rel):
                    new_row = translate(tuple(row))
                    assert len(new_row) == arity
                    batch.append([self.uid(v) for v in new_row] + [1 if src else 0])
                    if len(batch) >= 2000:
                        self.conn.executemany(sql, batch)
                        batch = []
                if batch:
                    self.conn.executemany(sql, batch)
                after = self.conn.execute(
                    f"SELECT COUNT(*) FROM {_quote(tmp)}").fetchone()[0]
                self.conn.execute(f"DROP TABLE {_quote('f_' + rel)}")
                self.conn.execute(
                    f"ALTER TABLE {_quote(tmp)} RENAME TO {_quote('f_' + rel)}")
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {_quote('ix_' + rel + '_src')} "
                    f"ON {_quote('f_' + rel)} (src)")
                for name, cols_idx in self.indexes.get(rel, ()):
                    idx_cols = ", ".join(f"c{i}" for i in cols_idx)
                    self.conn.execute(
                        f"CREATE INDEX IF NOT EXISTS {_quote(name)} "
                        f"ON {_quote('f_' + rel)} ({idx_cols})")
                stats[rel] = {"before": int(before), "after": int(after),
                              "collapsed": int(before) - int(after)}
            self.commit()
        except BaseException:
            self.rollback()
            raise
        return stats

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def delete_files(self) -> None:
        self.close()
        if self.path != ":memory:":
            for suffix in ("", "-wal", "-shm", "-journal"):
                try:
                    os.remove(self.path + suffix)
                except OSError:
                    pass
