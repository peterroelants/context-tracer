import contextlib
import json
import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Optional, Self

from context_tracer.constants import END_TIME_KEY, NAME_KEY, START_TIME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.utils.json_encoder import AnyEncoder, JSONDictType
from context_tracer.utils.time_utils import get_local_timestamp


# Span #####################################################
class TraceSpanSqlite(TraceSpan, TraceTree):
    """
    TODO: Documentation
    Node in the trace tree.

    Note: The tree is build "backwards" from the leaves to the root. Nodes have references to their parent, but not to their children. The child is the latest context, while the parent is the previous context.
    """

    tracing: "TracingSqlite"
    _id: int

    def __init__(self, tracing: "TracingSqlite", db_id: int) -> None:
        self.tracing = tracing
        self._id = db_id
        super().__init__()

    @property
    def id(self) -> bytes:
        return str(self._id).encode()

    @property
    def name(self) -> str:
        with self.tracing.db_conn() as db_conn:
            name = get_name(db_conn, self._id)
        return name

    @property
    def data(self) -> JSONDictType:
        with self.tracing.db_conn() as db_conn:
            data_json = get_data(db_conn, self._id)
        data = json.loads(data_json)
        return data

    @classmethod
    def new(
        cls,
        tracing: "TracingSqlite",
        name: str,
        data: dict,
        parent_id: int | None,
    ) -> Self:
        data_json: str = json.dumps(data, cls=AnyEncoder)
        with tracing.db_conn() as db_conn:
            node_id = create_row(
                db_conn,
                parent_id=parent_id,
                name=name,
                data_json=data_json,
            )
        assert node_id is not None
        return cls(tracing=tracing, db_id=node_id)

    @property
    def parent(self: Self) -> Optional[Self]:
        with self.tracing.db_conn() as db_conn:
            parent_id = get_parent_id(db_conn, self._id)
        if parent_id is None:
            return None
        return self.__class__(tracing=self.tracing, db_id=parent_id)

    @property
    def children(self: Self) -> list[Self]:
        with self.tracing.db_conn() as db_conn:
            children_ids = get_children_ids(db_conn, self._id)
        return [
            self.__class__(tracing=self.tracing, db_id=child_id)
            for child_id in children_ids
        ]

    def new_child(self: Self, **data) -> Self:
        name = data.pop(NAME_KEY, "no-name")
        return self.new(tracing=self.tracing, name=name, data=data, parent_id=self._id)

    def update_data(self, **new_data) -> None:
        data = self.data
        data.update(new_data)
        data_json: str = json.dumps(data, cls=AnyEncoder)
        with self.tracing.db_conn() as db_conn:
            update_row(db_conn, self._id, data_json)

    def __enter__(self: Self) -> Self:
        start_time = get_local_timestamp().isoformat(sep=" ", timespec="seconds")
        self.update_data(**{START_TIME_KEY: start_time})
        return self

    def __exit__(self, *exc) -> None:
        end_time = get_local_timestamp().isoformat(sep=" ", timespec="seconds")
        self.update_data(**{END_TIME_KEY: end_time})
        return None


class TracingSqlite(Tracing[TraceSpanSqlite, TraceSpanSqlite]):
    def __init__(self, db_path: Path):
        self.db_path = db_path
        init_db(self.db_path)

    @contextlib.contextmanager
    def db_conn(self) -> Iterator[sqlite3.Connection]:
        with connect_db(self.db_path) as db_conn:
            yield db_conn

    @property
    def root_span(self) -> TraceSpanSqlite:
        return self._table_root

    @property
    def _table_root(self) -> TraceSpanSqlite:
        """Get the root node from the database or create a new one if it does not exist."""
        with self.db_conn() as db_conn:
            root_id = get_root_id(db_conn)
        if root_id is not None:
            return TraceSpanSqlite(self, db_id=root_id)
        return TraceSpanSqlite.new(
            tracing=self,
            name="root",
            data={},
            parent_id=None,
        )

    @property
    def tree(self) -> TraceSpanSqlite:
        """Return a representable version of the root of the trace tree."""
        return self.root_span


# SQL ##############################################################
TABLE_NAME = "trace_spans"


@contextlib.contextmanager
def connect_db(db_path: Path) -> Iterator[sqlite3.Connection]:
    sqlite_db_path = str(db_path.resolve())
    logging.info(f"Using database path {sqlite_db_path=}.")
    with sqlite3.connect(sqlite_db_path) as conn:
        yield conn


def init_db(db_path: Path) -> None:
    """Initialize the database."""
    CREATE_TABLE_SQL = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
        "id INTEGER PRIMARY KEY"
        ", parent_id INTEGER"
        ", name TEXT NOT NULL"
        ", data_json TEXT NOT NULL);"
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Initializing database...")
    with connect_db(db_path) as db_conn:
        cursor = db_conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        db_conn.commit()
    logging.info(f"Database initialized {db_conn=}.")


def create_row(
    db_conn: sqlite3.Connection,
    parent_id: int | None,
    name: str,
    data_json: str,
) -> int:
    """Create a new row in the database table and return its id."""
    INSERT_ROW_SQL = (
        f"INSERT INTO {TABLE_NAME} (parent_id, name, data_json) VALUES (?, ?, ?);"
    )
    cursor = db_conn.cursor()
    cursor.execute(INSERT_ROW_SQL, (parent_id, name, data_json))
    db_conn.commit()
    row_id = cursor.lastrowid
    assert row_id is not None
    return row_id


def get_root_id(db_conn: sqlite3.Connection) -> int | None:
    """Get the id of the root row in the database table."""
    GET_ROOT_ID_SQL = f"SELECT id FROM {TABLE_NAME} WHERE parent_id IS NULL;"
    cursor = db_conn.cursor()
    cursor.execute(GET_ROOT_ID_SQL)
    root_ids = list(cursor.fetchall())
    if len(root_ids) > 1:
        raise ValueError(f"Expected at most one root node, got {root_ids=}.")
    if len(root_ids) == 0:
        return None
    root_id = root_ids[0][0]
    assert root_id is not None
    return root_id


def get_name(db_conn: sqlite3.Connection, row_id: int) -> str:
    """Get the name of a row in the database table."""
    GET_NAME_SQL = f"SELECT name FROM {TABLE_NAME} WHERE id = ?;"
    cursor = db_conn.cursor()
    cursor.execute(GET_NAME_SQL, (row_id,))
    name = cursor.fetchone()[0]
    assert name is not None
    return name


def get_data(db_conn: sqlite3.Connection, row_id: int) -> str:
    """Get the data of a row in the database table."""
    GET_DATA_SQL = f"SELECT data_json FROM {TABLE_NAME} WHERE id = ?;"
    cursor = db_conn.cursor()
    cursor.execute(GET_DATA_SQL, (row_id,))
    data_json = cursor.fetchone()[0]
    assert data_json is not None
    return data_json


def get_parent_id(db_conn: sqlite3.Connection, row_id: int) -> int | None:
    """Get the parent_id of a row in the database table."""
    GET_PARENT_ID_SQL = f"SELECT parent_id FROM {TABLE_NAME} WHERE id = ?;"
    cursor = db_conn.cursor()
    cursor.execute(GET_PARENT_ID_SQL, (row_id,))
    parent_id = cursor.fetchone()[0]
    return parent_id


def get_children_ids(db_conn: sqlite3.Connection, row_id: int) -> list[int]:
    """Get the children_ids of a row in the database table."""
    GET_CHILDREN_IDS_SQL = f"SELECT id FROM {TABLE_NAME} WHERE parent_id = ?;"
    cursor = db_conn.cursor()
    cursor.execute(GET_CHILDREN_IDS_SQL, (row_id,))
    children_ids = [row[0] for row in cursor.fetchall()]
    return children_ids


def update_row(db_conn: sqlite3.Connection, row_id: int, data_json: str) -> None:
    """Update the data of a row in the database table."""
    UPDATE_ROW_SQL = f"UPDATE {TABLE_NAME} SET data_json = ? WHERE id = ?;"
    cursor = db_conn.cursor()
    cursor.execute(UPDATE_ROW_SQL, (data_json, row_id))
    db_conn.commit()
