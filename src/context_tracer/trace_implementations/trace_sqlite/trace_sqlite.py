import json
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Final, Self

from context_tracer.constants import NAME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.utils.id_utils import new_uid
from context_tracer.utils.json_encoder import AnyEncoder, JSONDictType

from .span_db import SpanDataBase

DEFAULT_SPAN_NAME: Final[str] = "no-name"


# Span #####################################################
class TraceSpanSqlite(TraceSpan, TraceTree, AbstractContextManager):
    """
    TODO: Documentation
    Node in the trace tree.

    Note: The tree is build "backwards" from the leaves to the root. Nodes have references to their parent, but not to their children. The child is the latest context, while the parent is the previous context.
    """

    span_db: SpanDataBase
    _span_uid: bytes

    def __init__(self, span_db: SpanDataBase, span_uid: bytes) -> None:
        self.span_db = span_db
        self._span_uid = span_uid
        super().__init__()

    @property
    def uid(self) -> bytes:
        return self._span_uid

    @property
    def name(self) -> str:
        return self.span_db.get_name(uid=self._span_uid)

    @property
    def data(self) -> JSONDictType:
        return json.loads(self.span_db.get_data_json(uid=self._span_uid))

    @property
    def parent(self: Self) -> Self | None:
        parent_uid = self.span_db.get_parent_uid(uid=self._span_uid)
        if parent_uid is None:
            return None
        return self.__class__(span_db=self.span_db, span_uid=parent_uid)

    @property
    def children(self: Self) -> list[Self]:
        children_uids = self.span_db.get_children_uids(uid=self._span_uid)
        return [
            self.__class__(span_db=self.span_db, span_uid=child_uid)
            for child_uid in children_uids
        ]

    @classmethod
    def new(
        cls,
        span_db: SpanDataBase,
        name: str,
        data: dict[str, Any],
        parent_uid: bytes | None,
    ) -> Self:
        data_json: str = json.dumps(data, cls=AnyEncoder)
        span_uid = new_uid()
        span_db.insert(
            uid=span_uid,
            name=name,
            data_json=data_json,
            parent_uid=parent_uid,
        )
        return cls(span_db=span_db, span_uid=span_uid)

    def new_child(self: Self, **data) -> Self:
        name = data.pop(NAME_KEY, DEFAULT_SPAN_NAME)
        return self.new(
            span_db=self.span_db, name=name, data=data, parent_uid=self._span_uid
        )

    def update_data(self, **new_data) -> None:
        data_json: str = json.dumps(new_data, cls=AnyEncoder)
        self.span_db.update_data_json(uid=self._span_uid, data_json=data_json)


class TracingSqlite(Tracing[TraceSpanSqlite, TraceSpanSqlite]):
    span_db: SpanDataBase

    def __init__(self, db_path: Path):
        self.span_db = SpanDataBase(db_path=db_path)

    @property
    def root_span(self) -> TraceSpanSqlite:
        return self._table_root

    @property
    def _table_root(self) -> TraceSpanSqlite:
        """Get the root node from the database or create a new one if it does not exist."""
        root_uids = self.span_db.get_root_uids()
        if len(root_uids) == 1:
            return TraceSpanSqlite(span_db=self.span_db, span_uid=root_uids[0])
        elif len(root_uids) == 0:
            return TraceSpanSqlite.new(
                span_db=self.span_db,
                name="root",
                data={},
                parent_uid=None,
            )
        else:  # len(root_spans) > 1
            raise ValueError(f"No singular node found: {root_uids=!r}!")

    @property
    def tree(self) -> TraceSpanSqlite:
        """Return a representable version of the root of the trace tree."""
        return self.root_span
