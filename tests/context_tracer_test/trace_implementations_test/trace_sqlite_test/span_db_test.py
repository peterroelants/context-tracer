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
        id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert(**span.model_dump())
    retrieved_span = tmp_db.get_span(id=span.id)
    assert retrieved_span == span
    assert tmp_db.get_name(id=span.id) == span.name
    assert tmp_db.get_parent_id(id=span.id) == span.parent_id is None
    assert tmp_db.get_data_json(id=span.id) == span.data_json
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(id=span.id) == []


def test_insert_or_update(tmp_db: SpanDataBase) -> None:
    span = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(id=span.id) == span
    assert tmp_db.get_name(id=span.id) == span.name
    assert tmp_db.get_parent_id(id=span.id) == span.parent_id is None
    assert tmp_db.get_data_json(id=span.id) == span.data_json
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(id=span.id) == []
    # Update with same span should not change anything
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(id=span.id) == span
    assert tmp_db.get_name(id=span.id) == span.name
    assert tmp_db.get_parent_id(id=span.id) == span.parent_id is None
    assert tmp_db.get_data_json(id=span.id) == span.data_json
    assert tmp_db.get_root_ids() == [span.id]
    assert tmp_db.get_children_ids(id=span.id) == []


def test_get_root_id_empty_db(tmp_db: SpanDataBase) -> None:
    assert tmp_db.get_root_ids() == []
    assert tmp_db.get_root_spans() == []


def test_multiple_roots(tmp_db: SpanDataBase) -> None:
    # This should ideally not happen, but we should be able to handle it
    parent_span1 = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="parent_1",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    parent_span2 = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="parent_2",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    tmp_db.insert_or_update(**parent_span1.model_dump())
    tmp_db.insert_or_update(**parent_span2.model_dump())
    assert set(tmp_db.get_root_ids()) == set([parent_span1.id, parent_span2.id])
    assert set(tmp_db.get_root_spans()) == set([parent_span1, parent_span2])


def test_parent_child(tmp_db: SpanDataBase) -> None:
    parent_span = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="parent",
        data_json='{"test_key": "test_val"}',
        parent_id=None,
    )
    child1_span = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="child_1",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.id,
    )
    child2_span = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="child_2",
        data_json='{"test_key": "test_val"}',
        parent_id=parent_span.id,
    )
    tmp_db.insert_or_update(**parent_span.model_dump())
    tmp_db.insert_or_update(**child1_span.model_dump())
    tmp_db.insert_or_update(**child2_span.model_dump())
    # Checks
    assert tmp_db.get_root_ids() == [parent_span.id]
    assert set(tmp_db.get_children_ids(id=parent_span.id)) == set(
        [
            child1_span.id,
            child2_span.id,
        ]
    )
    assert tmp_db.get_children_ids(id=child1_span.id) == []
    assert tmp_db.get_children_ids(id=child2_span.id) == []
    assert tmp_db.get_parent_id(id=parent_span.id) is None
    assert tmp_db.get_parent_id(id=child1_span.id) == parent_span.id
    assert tmp_db.get_parent_id(id=child2_span.id) == parent_span.id


def test_update_data(tmp_db: SpanDataBase) -> None:
    data_1 = dict(name="data_1", a_specific=1, common=dict(a=1, b=2))
    data_2 = dict(name="data_2", b_specific=22, common=dict(b=20, c=30))
    span = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="span",
        data_json=json.dumps(data_1),
        parent_id=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    tmp_db.update_data_json(id=span.id, data_json=json.dumps(data_2))
    # Checks
    data_json_merged = tmp_db.get_data_json(id=span.id)
    data_merged = json.loads(data_json_merged)
    assert data_merged["name"] == "data_2"
    assert data_merged["a_specific"] == 1
    assert data_merged["b_specific"] == 22
    assert data_merged["common"] == dict(a=1, b=20, c=30)


def test_get_span_ids_from_name(tmp_db: SpanDataBase) -> None:
    span_1 = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="span",
        data_json="{}",
        parent_id=None,
    )
    span_2 = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="span",
        data_json="{}",
        parent_id=None,
    )
    span_other = SpanDbRow(
        id=uuid.uuid4().bytes,
        name="span_other",
        data_json="{}",
        parent_id=None,
    )
    tmp_db.insert_or_update(**span_1.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.id]
    tmp_db.insert_or_update(**span_other.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.id]
    tmp_db.insert_or_update(**span_2.model_dump())
    assert set(tmp_db.get_span_ids_from_name(name="span")) == set(
        [span_1.id, span_2.id]
    )
