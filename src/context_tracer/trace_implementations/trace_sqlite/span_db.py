import contextlib
import json
import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Final

from pydantic import BaseModel

from context_tracer.utils.json_encoder import JSONDictType


class SpanDbRow(BaseModel):
    """Representation of a row in the database table."""

    id: bytes  # TODO: `id` to be closer to `TraceSpan`?
    name: str
    data_json: str
    parent_id: bytes | None

    @property
    def data(self) -> JSONDictType:
        return json.loads(self.data_json)

    def __hash__(self) -> int:
        return self.id.__hash__()


# SQL ##############################################################
TABLE_NAME: Final[str] = "trace_spans"
ID_KEY: Final[str] = "id"
PARENT_ID_KEY: Final[str] = "parent_id"
NAME_KEY: Final[str] = "name"
DATA_KEY: Final[str] = "data_json"


# TODO: Test if this is serializable
class SpanDataBase:
    """
    Database for storing spans.
    """

    db_path: Path

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path.resolve()
        self.init_db()

    @contextlib.contextmanager
    def connect_db(self) -> Iterator[sqlite3.Connection]:
        with sqlite3.connect(str(self.db_path)) as conn:
            yield conn

    def init_db(self) -> None:
        """Initialize the database."""
        CREATE_TABLE_SQL = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                {ID_KEY} BLOB PRIMARY KEY,
                {PARENT_ID_KEY} BLOB,
                {NAME_KEY} TEXT NOT NULL,
                {DATA_KEY} TEXT NOT NULL
            ) WITHOUT ROWID;
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logging.debug("Initializing database...")
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(CREATE_TABLE_SQL)
            db_conn.commit()
        assert self.db_path.exists()
        logging.info(f"Database initialized at {self.db_path!r}.")

    def insert(
        self,
        id: bytes,
        name: str,
        data_json: str,
        parent_id: bytes | None,
    ) -> None:
        """Create a new row in the database table and return its id."""
        INSERT_ROW_SQL = f"""
            INSERT INTO {TABLE_NAME} (
                {ID_KEY}, {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?);
        """
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(
                INSERT_ROW_SQL,
                (id, parent_id, name, data_json),
            )
            db_conn.commit()

    def insert_or_update(
        self,
        id: bytes,
        name: str,
        data_json: str,
        parent_id: bytes | None,
    ) -> None:
        """
        Insert or update the data of a row in the database table.

        Uses UPSERT: https://www.sqlite.org/draft/lang_UPSERT.html
        """
        UPDATE_ROW_SQL = f"""
            INSERT INTO {TABLE_NAME} (
                {ID_KEY}, {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?)
            ON CONFLICT ({ID_KEY}) DO UPDATE SET
                {NAME_KEY} = excluded.{NAME_KEY},
                {DATA_KEY} = excluded.{DATA_KEY};
        """
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(
                UPDATE_ROW_SQL,
                (id, parent_id, name, data_json),
            )

    def get_span(self, id: bytes) -> SpanDbRow:
        """Get the span corresponding to the given id."""
        GET_SPAN_SQL = f"""
            SELECT {ID_KEY}, {PARENT_ID_KEY}, {NAME_KEY}, {DATA_KEY}
            FROM {TABLE_NAME} WHERE {ID_KEY} = ?;
        """
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_SPAN_SQL, (id,))
            row = cursor.fetchone()
            assert row is not None
            span = SpanDbRow(
                id=row[0],
                parent_id=row[1],
                name=row[2],
                data_json=row[3],
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

    # TODO: Still needed
    def get_root_spans(self) -> list[SpanDbRow]:
        """Get the parents of a row in the database table."""
        GET_ROOT_SPANS_SQL = f"""
            SELECT {ID_KEY}, {NAME_KEY}, {PARENT_ID_KEY}, {DATA_KEY}
            FROM {TABLE_NAME} WHERE {ID_KEY} IN (
                SELECT {ID_KEY} FROM {TABLE_NAME} WHERE {PARENT_ID_KEY} IS NULL
            );
        """
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_ROOT_SPANS_SQL)
            root_vars = list(cursor.fetchall())
            print(f"{root_vars=}")
            return [
                SpanDbRow(
                    id=row[0],
                    name=row[1],
                    parent_id=row[2],
                    data_json=row[3],
                )
                for row in root_vars
            ]

    def get_children_ids(self, id: bytes) -> list[bytes]:
        """Get the children_ids of a row in the database table."""
        GET_CHILDREN_IDS_SQL = f"SELECT id FROM {TABLE_NAME} WHERE {PARENT_ID_KEY} = ?;"
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_CHILDREN_IDS_SQL, (id,))
            children_ids = [row[0] for row in cursor.fetchall()]
            return children_ids

    # TODO: Update json_data using sqlite json functions
    # https://dadroit.com/blog/json-querying/
    def update_data_json(self, id: bytes, data_json: str) -> None:
        """
        Update the data of a row in the database table.

        New data will recursively overwrite old data by following the json patch standard.
        More info:
        - https://www.sqlite.org/json1.html#jpatch
        - https://jsonpatch.com/
        """
        UPDATE_DATA_JSON_SQL = f"""
            UPDATE {TABLE_NAME}
            SET {DATA_KEY} = json_patch({DATA_KEY}, ?)
            WHERE {ID_KEY} = ?;
        """
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(UPDATE_DATA_JSON_SQL, (data_json, id))
            db_conn.commit()

    def get_data_json(self, id: bytes) -> str:
        """Get the data of a row in the database table."""
        GET_DATA_SQL = f"SELECT {DATA_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_DATA_SQL, (id,))
            data_json = cursor.fetchone()[0]
            assert data_json is not None
            return data_json

    def get_name(self, id: bytes) -> str:
        """Get the name of a row in the database table."""
        GET_NAME_SQL = f"SELECT {NAME_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_NAME_SQL, (id,))
            name = cursor.fetchone()[0]
            assert name is not None
            return name

    def get_parent_id(self, id: bytes) -> bytes | None:
        """Get the parent_id of a row in the database table."""
        GET_PARENT_ID_SQL = (
            f"SELECT {PARENT_ID_KEY} FROM {TABLE_NAME} WHERE {ID_KEY} = ?;"
        )
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_PARENT_ID_SQL, (id,))
            parent_id = cursor.fetchone()[0]
            return parent_id

    def get_span_ids_from_name(self, name: str) -> list[bytes]:
        """Get all span ids with the given name."""
        GET_SPAN_IDS_FROM_NAME_SQL = (
            f"SELECT {ID_KEY} FROM {TABLE_NAME} WHERE {NAME_KEY} = ?;"
        )
        with self.connect_db() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(GET_SPAN_IDS_FROM_NAME_SQL, (name,))
            span_ids = cursor.fetchall()
            return [row[0] for row in span_ids]
