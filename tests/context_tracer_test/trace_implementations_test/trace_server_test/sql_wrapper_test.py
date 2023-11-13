import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_server.sql_wrapper import (
    SpanData,
    SpanDataBase,
)


@pytest.fixture
def tmp_db() -> Iterator[SpanDataBase]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.db"
        db = SpanDataBase(db_path=db_path)
        yield db


def test_insert(tmp_db: SpanDataBase) -> None:
    span = SpanData(
        id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert(span=span)
    retrieved_span = tmp_db.get_span(span_id=span.id)
    assert retrieved_span == span
    assert tmp_db.get_data(span_id=span.id) == span.data
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(span_id=span.id) == []


def test_insert_or_update(tmp_db: SpanDataBase) -> None:
    span = SpanData(
        id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(span=span)
    assert tmp_db.get_span(span_id=span.id) == span
    assert tmp_db.get_data(span_id=span.id) == span.data
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(span_id=span.id) == []
    # Update with same span should not change anything
    tmp_db.insert_or_update(span=span)
    assert tmp_db.get_span(span_id=span.id) == span
    assert tmp_db.get_data(span_id=span.id) == span.data
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(span_id=span.id) == []


def test_get_root_id_empyt_db(tmp_db: SpanDataBase) -> None:
    assert tmp_db.get_root_ids() == []
    assert tmp_db.get_root_spans() == []


def test_multiple_roots(tmp_db: SpanDataBase) -> None:
    # This should ideally not happen, but we should be able to handle it
    parent_span1 = SpanData(
        id=uuid.uuid4().bytes,
        name="parent_1",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    parent_span2 = SpanData(
        id=uuid.uuid4().bytes,
        name="parent_2",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(span=parent_span1)
    tmp_db.insert_or_update(span=parent_span2)
    assert set(tmp_db.get_root_ids()) == set([parent_span1.id, parent_span2.id])
    assert set(tmp_db.get_root_spans()) == set([parent_span1, parent_span2])


def test_parent_child(tmp_db: SpanDataBase) -> None:
    parent_span = SpanData(
        id=uuid.uuid4().bytes,
        name="parent",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    child1_span = SpanData(
        id=uuid.uuid4().bytes,
        name="child_1",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.id,
    )
    child2_span = SpanData(
        id=uuid.uuid4().bytes,
        name="child_2",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.id,
    )
    tmp_db.insert_or_update(span=parent_span)
    tmp_db.insert_or_update(span=child1_span)
    tmp_db.insert_or_update(span=child2_span)
    # Checks
    assert tmp_db.get_root_ids() == [parent_span.id]
    assert set(tmp_db.get_children_ids(span_id=parent_span.id)) == set(
        [
            child1_span.id,
            child2_span.id,
        ]
    )
