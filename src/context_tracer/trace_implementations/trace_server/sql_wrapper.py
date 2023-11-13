import contextlib
import json
import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from textwrap import dedent
from typing import Any, Final

from pydantic import BaseModel


class SpanData(BaseModel):
    id: bytes
    name: str
    data_json: str
    parent_id: bytes | None

    @property
    def data(self) -> dict[str, Any]:
        return json.loads(self.data_json)

    def __hash__(self) -> int:
        return self.id.__hash__()


# TODO: SpanTree in different file?
class SpanTree(BaseModel):  # TraceTree
    id: bytes
    name: str
    parent_id: bytes | None
    db_path: Path
    _maybe_span_db: "SpanDataBase | None" = None

    @property
    def span_db(self) -> "SpanDataBase":
        if self._maybe_span_db is None:
            self._maybe_span_db = SpanDataBase(db_path=self.db_path)
        return self._maybe_span_db

    @property
    def data(self) -> dict[str, Any]:
        return self.span_db.get_data(span_id=self.id)

    @property
    def parent(self) -> "SpanTree | None":
        if self.parent_id is None:
            return None
        parent = self.from_span_data(
            self.span_db.get_span(span_id=self.parent_id), db_path=self.db_path
        )
        return parent

    @property
    def children(self) -> list["SpanTree"]:
        children_ids = self.span_db.get_children_ids(span_id=self.id)
        children = [
            self.from_span_data(
                self.span_db.get_span(span_id=child_id), db_path=self.db_path
            )
            for child_id in children_ids
        ]
        return children

    @classmethod
    def from_span_data(cls, span_data: SpanData, db_path: Path) -> "SpanTree":
        return cls(db_path=db_path, **span_data.model_dump())

    # TODO: Tracing?
    # @classmethod
    # def root_span(cls) -> "SpanTree":
    #     root_id = self.span_db.get_root_id()
    #     if root_id is None:
    #         raise ValueError("No root node found!")
    #     root = cls.from_span_data(
    #         get_span(span_id=root_id, db_conn=db_conn), db_path=db_path
    #     )
    #     return root


def get_span_tree_root(span_db: "SpanDataBase") -> SpanTree:
    root_ids = span_db.get_root_ids()
    if len(root_ids) != 1:
        raise ValueError(f"No singular node found: {root_ids=!r}!")
    root_id = root_ids[0]
    root = SpanTree.from_span_data(
        span_data=span_db.get_span(span_id=root_id), db_path=span_db.db_path
    )
    return root


# TODO: dedent & strip needed?
# SQL ##############################################################
TABLE_NAME: Final[str] = "trace_spans"
ID_KEY: Final[str] = "id"
PARENT_ID_KEY: Final[str] = "parent_id"
NAME_KEY: Final[str] = "name"
DATA_KEY: Final[str] = "data_json"


# TODO: Provide DB class
class SpanDataBase:
    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.init_db()

    @contextlib.contextmanager
    def connect_db(self) -> Iterator[sqlite3.Connection]:
        sqlite_db_path = str(self.db_path.resolve())
        logging.info(f"Using database path {sqlite_db_path=}.")
        with sqlite3.connect(sqlite_db_path) as conn:
            yield conn

    def init_db(self) -> None:
        """Initialize the database."""
        CREATE_TABLE_SQL = dedent(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                {ID_KEY} BLOB PRIMARY KEY,
                {PARENT_ID_KEY} BLOB,
                {NAME_KEY} TEXT NOT NULL,
                {DATA_KEY} TEXT NOT NULL
            ) WITHOUT ROWID;
        """
        ).strip()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logging.debug("Initializing database...")
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(CREATE_TABLE_SQL)
            db_conn.commit()
        logging.info(f"Database initialized {db_conn=}.")

    def insert(self, span: SpanData) -> None:
        """Create a new row in the database table and return its id."""
        INSERT_ROW_SQL = dedent(
            f"""
            INSERT INTO {TABLE_NAME} (
                {ID_KEY}, {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?);
        """
        ).strip()
        data_json: str = json.dumps(span.data)
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(
                INSERT_ROW_SQL,
                (span.id, span.parent_id, span.name, data_json),
            )
            db_conn.commit()

    def insert_or_update(self, span: SpanData) -> None:
        """
        Insert or update the data of a row in the database table.

        Uses UPSERT: https://www.sqlite.org/draft/lang_UPSERT.html
        """
        UPDATE_ROW_SQL = dedent(
            f"""
            INSERT INTO {TABLE_NAME} (
                {ID_KEY}, {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?)
            ON CONFLICT ({ID_KEY}) DO UPDATE SET
                {NAME_KEY} = excluded.{NAME_KEY},
                {DATA_KEY} = excluded.{DATA_KEY};
            """
        ).strip()
        data_json: str = json.dumps(span.data)
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(
                UPDATE_ROW_SQL,
                (span.id, span.parent_id, span.name, data_json),
            )

    def get_span(self, span_id: bytes) -> SpanData:
        """Get the data of a row in the database table."""
        GET_SPAN_SQL = dedent(
            f"""
            SELECT {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            FROM {TABLE_NAME} WHERE {ID_KEY} = ?;
            """
        ).strip()
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_SPAN_SQL, (span_id,))
            row = cursor.fetchone()
            assert row is not None
            span = SpanData(
                id=span_id,
                parent_id=row[0],
                name=row[1],
                data_json=row[2],
            )
            return span

    def get_root_ids(self) -> list[bytes]:
        """Get the id of the root row in the database table."""
        GET_ROOT_ID_SQL = (
            f"SELECT {ID_KEY} FROM {TABLE_NAME} WHERE {PARENT_ID_KEY} IS NULL;"
        )
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_ROOT_ID_SQL)
            root_ids = list(cursor.fetchall())
            return [row[0] for row in root_ids]

    def get_root_spans(self) -> list[SpanData]:
        """Get the parents of a row in the database table."""
        GET_ROOT_SPANS_SQL = dedent(
            f"""
            SELECT {ID_KEY}, {NAME_KEY}, {PARENT_ID_KEY}, {DATA_KEY}
            FROM {TABLE_NAME} WHERE {ID_KEY} IN (
                SELECT {ID_KEY} FROM {TABLE_NAME} WHERE {PARENT_ID_KEY} IS NULL
            );
        """.strip()
        )
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_ROOT_SPANS_SQL)
            root_vars = list(cursor.fetchall())
            print(f"{root_vars=}")
            return [
                SpanData(
                    id=row[0],
                    name=row[1],
                    parent_id=row[2],
                    data_json=row[3],
                )
                for row in root_vars
            ]

    def get_children_ids(self, span_id: bytes) -> list[bytes]:
        """Get the children_ids of a row in the database table."""
        GET_CHILDREN_IDS_SQL = f"SELECT id FROM {TABLE_NAME} WHERE {PARENT_ID_KEY} = ?;"
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_CHILDREN_IDS_SQL, (span_id,))
            children_ids = [row[0] for row in cursor.fetchall()]
            return children_ids

    # TODO: Update json_data using sqlite json functions
    def update_data(self, span: SpanData) -> None:
        """Update the data of a row in the database table."""
        UPDATE_ROW_SQL = f"UPDATE {TABLE_NAME} SET {DATA_KEY} = ? WHERE {ID_KEY} = ?;"
        data_json: str = json.dumps(span.data)
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(UPDATE_ROW_SQL, (data_json, span.id))
            db_conn.commit()

    # TODO : Remove?
    # def get_name(self, span_id: bytes) -> str:
    #     """Get the name of a row in the database table."""
    #     # TODO: Is this ever used?
    #     GET_NAME_SQL = f"SELECT {NAME_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
    #     with self.connect_db() as db_conn:
    #         cursor = db_conn.cursor()
    #         cursor.execute(GET_NAME_SQL, (span_id,))
    #         name = cursor.fetchone()[0]
    #         assert name is not None
    #         return name

    def get_data(self, span_id: bytes) -> dict[str, Any]:
        """Get the data of a row in the database table."""
        # TODO: Is this ever used?
        GET_DATA_SQL = f"SELECT {DATA_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_DATA_SQL, (span_id,))
            data_json = cursor.fetchone()[0]
            assert data_json is not None
            return json.loads(data_json)

    # def get_parent_id(
    #     self,
    #     span_id: bytes,
    # ) -> bytes | None:
    #     """Get the parent_id of a row in the database table."""
    #     GET_PARENT_ID_SQL = f"SELECT {PARENT_ID_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
    #     with self.connect_db() as db_conn:
    #         cursor = db_conn.cursor()
    #         cursor.execute(GET_PARENT_ID_SQL, (span_id,))
    #         parent_id = cursor.fetchone()[0]
    #         return parent_id
