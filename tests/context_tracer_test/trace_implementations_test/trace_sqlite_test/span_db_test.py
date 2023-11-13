import json
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_sqlite.span_db import (
    SpanDataBase,
    SpanDbRow,
)


@pytest.fixture
def tmp_db() -> Iterator[SpanDataBase]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.db"
        db = SpanDataBase(db_path=db_path)
        yield db


def test_insert(tmp_db: SpanDataBase) -> None:
    span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert(**span.model_dump())
    retrieved_span = tmp_db.get_span(span_id=span.span_id)
    assert retrieved_span == span
    assert tmp_db.get_name(span_id=span.span_id) == span.name
    assert tmp_db.get_parent_id(span_id=span.span_id) == span.parent_id is None
    assert tmp_db.get_data_json(span_id=span.span_id) == span.data_json
    assert tmp_db.get_root_ids() == [span.span_id]
    assert tmp_db.get_children_ids(span_id=span.span_id) == []


def test_insert_or_update(tmp_db: SpanDataBase) -> None:
    span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(span_id=span.span_id) == span
    assert tmp_db.get_name(span_id=span.span_id) == span.name
    assert tmp_db.get_parent_id(span_id=span.span_id) == span.parent_id is None
    assert tmp_db.get_data_json(span_id=span.span_id) == span.data_json
    assert tmp_db.get_root_ids() == [span.span_id]
    assert tmp_db.get_children_ids(span_id=span.span_id) == []
    # Update with same span should not change anything
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(span_id=span.span_id) == span
    assert tmp_db.get_name(span_id=span.span_id) == span.name
    assert tmp_db.get_parent_id(span_id=span.span_id) == span.parent_id is None
    assert tmp_db.get_data_json(span_id=span.span_id) == span.data_json
    assert tmp_db.get_root_ids() == [span.span_id]
    assert tmp_db.get_children_ids(span_id=span.span_id) == []


def test_get_root_id_empty_db(tmp_db: SpanDataBase) -> None:
    assert tmp_db.get_root_ids() == []
    assert tmp_db.get_root_spans() == []


def test_multiple_roots(tmp_db: SpanDataBase) -> None:
    # This should ideally not happen, but we should be able to handle it
    parent_span1 = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="parent_1",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    parent_span2 = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="parent_2",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(**parent_span1.model_dump())
    tmp_db.insert_or_update(**parent_span2.model_dump())
    assert set(tmp_db.get_root_ids()) == set(
        [parent_span1.span_id, parent_span2.span_id]
    )
    assert set(tmp_db.get_root_spans()) == set([parent_span1, parent_span2])


def test_parent_child(tmp_db: SpanDataBase) -> None:
    parent_span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="parent",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    child1_span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="child_1",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.span_id,
    )
    child2_span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="child_2",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.span_id,
    )
    tmp_db.insert_or_update(**parent_span.model_dump())
    tmp_db.insert_or_update(**child1_span.model_dump())
    tmp_db.insert_or_update(**child2_span.model_dump())
    # Checks
    assert tmp_db.get_root_ids() == [parent_span.span_id]
    assert set(tmp_db.get_children_ids(span_id=parent_span.span_id)) == set(
        [
            child1_span.span_id,
            child2_span.span_id,
        ]
    )
    assert tmp_db.get_children_ids(span_id=child1_span.span_id) == []
    assert tmp_db.get_children_ids(span_id=child2_span.span_id) == []
    assert tmp_db.get_parent_id(span_id=parent_span.span_id) is None
    assert tmp_db.get_parent_id(span_id=child1_span.span_id) == parent_span.span_id
    assert tmp_db.get_parent_id(span_id=child2_span.span_id) == parent_span.span_id


def test_update_data(tmp_db: SpanDataBase) -> None:
    data_1 = dict(name="data_1", a_specific=1, common=dict(a=1, b=2))
    data_2 = dict(name="data_2", b_specific=22, common=dict(b=20, c=30))
    span = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="span",
        data_json=json.dumps(data_1),
        parent_id=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    tmp_db.update_data_json(span_id=span.span_id, data_json=json.dumps(data_2))
    # Checks
    data_json_merged = tmp_db.get_data_json(span_id=span.span_id)
    data_merged = json.loads(data_json_merged)
    assert data_merged["name"] == "data_2"
    assert data_merged["a_specific"] == 1
    assert data_merged["b_specific"] == 22
    assert data_merged["common"] == dict(a=1, b=20, c=30)


def test_get_span_ids_from_name(tmp_db: SpanDataBase) -> None:
    span_1 = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="span",
        data_json="{}",
        parent_id=None,
    )
    span_2 = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="span",
        data_json="{}",
        parent_id=None,
    )
    span_other = SpanDbRow(
        span_id=uuid.uuid4().bytes,
        name="span_other",
        data_json="{}",
        parent_id=None,
    )
    tmp_db.insert_or_update(**span_1.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.span_id]
    tmp_db.insert_or_update(**span_other.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.span_id]
    tmp_db.insert_or_update(**span_2.model_dump())
    assert set(tmp_db.get_span_ids_from_name(name="span")) == set(
        [span_1.span_id, span_2.span_id]
    )
