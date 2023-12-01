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

    uid: bytes
    name: str
    data_json: str
    parent_uid: bytes | None

    @property
    def data(self) -> JSONDictType:
        return json.loads(self.data_json)

    def __hash__(self) -> int:
        return self.uid.__hash__()


# SQL ##############################################################
TABLE_NAME: Final[str] = "trace_spans"
UID_KEY: Final[str] = "uid"
PARENT_UID_KEY: Final[str] = "parent_uid"
NAME_KEY: Final[str] = "name"
DATA_KEY: Final[str] = "data_json"
UPDATED_TIME_KEY: Final[str] = "timestamp_last_updated"


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

    @contextlib.contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        with self.connect_db() as db_conn:
            with contextlib.closing(db_conn.cursor()) as cursor:
                yield cursor

    def init_db(self) -> None:
        """Initialize the database."""
        # Initialize the database
        # UID is primary key
        # Create a trigger to timestamp the last update
        CREATE_TABLE_SQL = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                {UID_KEY} BLOB PRIMARY KEY,
                {PARENT_UID_KEY} BLOB,
                {NAME_KEY} TEXT NOT NULL,
                {DATA_KEY} TEXT NOT NULL,
                {UPDATED_TIME_KEY} FLOAT
            ) WITHOUT ROWID;
        """
        CREATE_TIMESTAMP_INSERT_TRIGGER_SQL = f"""
        CREATE TRIGGER IF NOT EXISTS on_insert_update_{UPDATED_TIME_KEY}
            AFTER INSERT ON {TABLE_NAME}
        BEGIN
            UPDATE {TABLE_NAME}
            SET {UPDATED_TIME_KEY} = unixepoch('now','subsec')
            WHERE {UID_KEY} = NEW.{UID_KEY};
        END;
        """
        CREATE_TIMESTAMP_UPDATE_TRIGGER_SQL = f"""
        CREATE TRIGGER IF NOT EXISTS on_update_update_{UPDATED_TIME_KEY}
            AFTER UPDATE OF {DATA_KEY}, {NAME_KEY}, {PARENT_UID_KEY} ON {TABLE_NAME}
        BEGIN
            UPDATE {TABLE_NAME}
            SET {UPDATED_TIME_KEY} = unixepoch('now','subsec')
            WHERE {UID_KEY} = NEW.{UID_KEY};
        END;
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logging.debug("Initializing database...")
        with self.cursor() as cursor:
            # Set Write-Ahead Logging (WAL) mode to enable concurrent reads and writes
            # https://www.sqlite.org/wal.html
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(CREATE_TIMESTAMP_INSERT_TRIGGER_SQL)
            cursor.execute(CREATE_TIMESTAMP_UPDATE_TRIGGER_SQL)
            cursor.connection.commit()
        assert self.db_path.exists()
        logging.info(f"Database initialized at {self.db_path!r}.")

    def insert(
        self,
        uid: bytes,
        name: str,
        data_json: str,
        parent_uid: bytes | None,
    ) -> None:
        """Create a new row in the database table and return its id."""
        INSERT_ROW_SQL = f"""
            INSERT INTO {TABLE_NAME} (
                {UID_KEY}, {PARENT_UID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?);
        """
        with self.cursor() as cursor:
            cursor.execute(
                INSERT_ROW_SQL,
                (uid, parent_uid, name, data_json),
            )
            cursor.connection.commit()

    def insert_or_update(
        self,
        uid: bytes,
        name: str,
        data_json: str,
        parent_uid: bytes | None,
    ) -> None:
        """
        Insert or update the data of a row in the database table.

        Uses UPSERT: https://www.sqlite.org/draft/lang_UPSERT.html
        """
        UPDATE_ROW_SQL = f"""
            INSERT INTO {TABLE_NAME} (
                {UID_KEY}, {PARENT_UID_KEY}, {NAME_KEY}, {DATA_KEY}
            )  VALUES (?, ?, ?, ?)
            ON CONFLICT ({UID_KEY}) DO UPDATE SET
                {NAME_KEY} = excluded.{NAME_KEY},
                {DATA_KEY} = excluded.{DATA_KEY};
        """
        with self.cursor() as cursor:
            cursor.execute(
                UPDATE_ROW_SQL,
                (uid, parent_uid, name, data_json),
            )
            cursor.connection.commit()

    def get_span(self, uid: bytes) -> SpanDbRow:
        """Get the span corresponding to the given id."""
        GET_SPAN_SQL = f"""
            SELECT {UID_KEY}, {PARENT_UID_KEY}, {NAME_KEY}, {DATA_KEY}
            FROM {TABLE_NAME} WHERE {UID_KEY} = ?;
        """
        with self.cursor() as cursor:
            cursor.execute(GET_SPAN_SQL, (uid,))
            row = cursor.fetchone()
        assert row is not None
        span = SpanDbRow(
            uid=row[0],
            parent_uid=row[1],
            name=row[2],
            data_json=row[3],
        )
        return span

    def get_root_uids(self) -> list[bytes]:
        """Get the uid of the root row in the database table."""
        GET_ROOT_ID_SQL = (
            f"SELECT {UID_KEY} FROM {TABLE_NAME} WHERE {PARENT_UID_KEY} IS NULL;"
        )
        with self.cursor() as cursor:
            cursor.execute(GET_ROOT_ID_SQL)
            root_uids = list(cursor.fetchall())
        return [row[0] for row in root_uids]

    def get_children_uids(self, uid: bytes) -> list[bytes]:
        """Get the children_ids of a row in the database table."""
        GET_CHILDREN_IDS_SQL = (
            f"SELECT {UID_KEY} FROM {TABLE_NAME} WHERE {PARENT_UID_KEY} = ?;"
        )
        with self.cursor() as cursor:
            cursor.execute(GET_CHILDREN_IDS_SQL, (uid,))
            child_rows = cursor.fetchall()
        return [row[0] for row in child_rows]

    def update_data_json(self, uid: bytes, data_json: str) -> None:
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
            WHERE {UID_KEY} = ?;
        """
        with self.cursor() as cursor:
            cursor.execute(UPDATE_DATA_JSON_SQL, (data_json, uid))
            cursor.connection.commit()

    def get_data_json(self, uid: bytes) -> str:
        """Get the data of a row in the database table."""
        GET_DATA_SQL = f"SELECT {DATA_KEY} FROM {TABLE_NAME} WHERE {UID_KEY} = ?;"
        with self.cursor() as cursor:
            cursor.execute(GET_DATA_SQL, (uid,))
            data_json = cursor.fetchone()[0]
        assert data_json is not None
        return data_json

    def get_name(self, uid: bytes) -> str:
        """Get the name of a row in the database table."""
        GET_NAME_SQL = f"SELECT {NAME_KEY} FROM {TABLE_NAME} WHERE {UID_KEY} = ?;"
        with self.cursor() as cursor:
            cursor.execute(GET_NAME_SQL, (uid,))
            name = cursor.fetchone()[0]
        assert name is not None
        return name

    def get_parent_uid(self, uid: bytes) -> bytes | None:
        """Get the parent_id of a row in the database table."""
        GET_PARENT_UID_SQL = (
            f"SELECT {PARENT_UID_KEY} FROM {TABLE_NAME} WHERE {UID_KEY} = ?;"
        )
        with self.cursor() as cursor:
            cursor.execute(GET_PARENT_UID_SQL, (uid,))
            parent_id = cursor.fetchone()[0]
        return parent_id

    def get_span_ids_from_name(self, name: str) -> list[bytes]:
        """Get all span ids with the given name."""
        GET_SPAN_UIDS_FROM_NAME_SQL = (
            f"SELECT {UID_KEY} FROM {TABLE_NAME} WHERE {NAME_KEY} = ?;"
        )
        with self.cursor() as cursor:
            cursor.execute(GET_SPAN_UIDS_FROM_NAME_SQL, (name,))
            rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_last_span_uid(self) -> bytes:
        """
        Get the uid of the last span in the database table.

        Assumes that the ids are ordered in ascending order, for example es generated by uuid7 or uui8.
        """
        GET_LAST_SPAN_UID_SQL = (
            f"SELECT {UID_KEY} FROM {TABLE_NAME} ORDER BY {UID_KEY} DESC LIMIT 1;"
        )
        with self.cursor() as cursor:
            cursor.execute(GET_LAST_SPAN_UID_SQL)
            row = cursor.fetchone()
        assert row is not None, "No spans in database."
        return row[0]

    def get_last_updated_span_uid(self) -> tuple[bytes, float]:
        """
        Get the uid of the last span in the database table.
        """
        GET_LAST_UPDATED_SPAN_UID_SQL = f"SELECT {UID_KEY}, {UPDATED_TIME_KEY} FROM {TABLE_NAME} ORDER BY {UPDATED_TIME_KEY} DESC LIMIT 1;"
        with self.cursor() as cursor:
            cursor.execute(GET_LAST_UPDATED_SPAN_UID_SQL)
            row = cursor.fetchone()
        assert row is not None, "No spans in database."
        return row[0], row[1]
