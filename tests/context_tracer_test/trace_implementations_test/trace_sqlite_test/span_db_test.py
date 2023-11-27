import json
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_sqlite.span_db import (
    SpanDataBase,
    SpanDbRow,
)
from context_tracer.utils.id_utils import new_uid


@pytest.fixture
def tmp_db() -> Iterator[SpanDataBase]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.sqlite"
        db = SpanDataBase(db_path=db_path)
        yield db


def test_insert(tmp_db: SpanDataBase) -> None:
    span = SpanDbRow(
        uid=new_uid(),
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_uid=None,
    )
    tmp_db.insert(**span.model_dump())
    retrieved_span = tmp_db.get_span(uid=span.uid)
    assert retrieved_span == span
    assert tmp_db.get_name(uid=span.uid) == span.name
    assert tmp_db.get_parent_uid(uid=span.uid) == span.parent_uid is None
    assert tmp_db.get_data_json(uid=span.uid) == span.data_json
    assert tmp_db.get_root_uids() == [span.uid]
    assert tmp_db.get_children_uids(uid=span.uid) == []


def test_insert_or_update(tmp_db: SpanDataBase) -> None:
    span = SpanDbRow(
        uid=new_uid(),
        name="test",
        data_json='{"test_key": "test_val"}',
        parent_uid=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(uid=span.uid) == span
    assert tmp_db.get_name(uid=span.uid) == span.name
    assert tmp_db.get_parent_uid(uid=span.uid) == span.parent_uid is None
    assert tmp_db.get_data_json(uid=span.uid) == span.data_json
    assert tmp_db.get_root_uids() == [span.uid]
    assert tmp_db.get_children_uids(uid=span.uid) == []
    # Update with same span should not change anything
    tmp_db.insert_or_update(**span.model_dump())
    assert tmp_db.get_span(uid=span.uid) == span
    assert tmp_db.get_name(uid=span.uid) == span.name
    assert tmp_db.get_parent_uid(uid=span.uid) == span.parent_uid is None
    assert tmp_db.get_data_json(uid=span.uid) == span.data_json
    assert tmp_db.get_root_uids() == [span.uid]
    assert tmp_db.get_children_uids(uid=span.uid) == []


def test_get_root_id_empty_db(tmp_db: SpanDataBase) -> None:
    assert tmp_db.get_root_uids() == []


def test_multiple_roots(tmp_db: SpanDataBase) -> None:
    # This should ideally not happen, but we should be able to handle it
    parent_span1 = SpanDbRow(
        uid=new_uid(),
        name="parent_1",
        data_json='{"test_key": "test_val"}',
        parent_uid=None,
    )
    parent_span2 = SpanDbRow(
        uid=new_uid(),
        name="parent_2",
        data_json='{"test_key": "test_val"}',
        parent_uid=None,
    )
    tmp_db.insert_or_update(**parent_span1.model_dump())
    tmp_db.insert_or_update(**parent_span2.model_dump())
    assert set(tmp_db.get_root_uids()) == set([parent_span1.uid, parent_span2.uid])


def test_parent_child(tmp_db: SpanDataBase) -> None:
    parent_span = SpanDbRow(
        uid=new_uid(),
        name="parent",
        data_json='{"test_key": "test_val"}',
        parent_uid=None,
    )
    child1_span = SpanDbRow(
        uid=new_uid(),
        name="child_1",
        data_json='{"test_key": "test_val"}',
        parent_uid=parent_span.uid,
    )
    child2_span = SpanDbRow(
        uid=new_uid(),
        name="child_2",
        data_json='{"test_key": "test_val"}',
        parent_uid=parent_span.uid,
    )
    tmp_db.insert_or_update(**parent_span.model_dump())
    tmp_db.insert_or_update(**child1_span.model_dump())
    tmp_db.insert_or_update(**child2_span.model_dump())
    # Checks
    assert tmp_db.get_root_uids() == [parent_span.uid]
    assert set(tmp_db.get_children_uids(uid=parent_span.uid)) == set(
        [
            child1_span.uid,
            child2_span.uid,
        ]
    )
    assert tmp_db.get_children_uids(uid=child1_span.uid) == []
    assert tmp_db.get_children_uids(uid=child2_span.uid) == []
    assert tmp_db.get_parent_uid(uid=parent_span.uid) is None
    assert tmp_db.get_parent_uid(uid=child1_span.uid) == parent_span.uid
    assert tmp_db.get_parent_uid(uid=child2_span.uid) == parent_span.uid


def test_update_data(tmp_db: SpanDataBase) -> None:
    data_1 = dict(name="data_1", a_specific=1, common=dict(a=1, b=2))
    data_2 = dict(name="data_2", b_specific=22, common=dict(b=20, c=30))
    span = SpanDbRow(
        uid=new_uid(),
        name="span",
        data_json=json.dumps(data_1),
        parent_uid=None,
    )
    tmp_db.insert_or_update(**span.model_dump())
    tmp_db.update_data_json(uid=span.uid, data_json=json.dumps(data_2))
    # Checks
    data_json_merged = tmp_db.get_data_json(uid=span.uid)
    data_merged = json.loads(data_json_merged)
    assert data_merged["name"] == "data_2"
    assert data_merged["a_specific"] == 1
    assert data_merged["b_specific"] == 22
    assert data_merged["common"] == dict(a=1, b=20, c=30)


def test_get_span_ids_from_name(tmp_db: SpanDataBase) -> None:
    span_1 = SpanDbRow(
        uid=new_uid(),
        name="span",
        data_json="{}",
        parent_uid=None,
    )
    span_2 = SpanDbRow(
        uid=new_uid(),
        name="span",
        data_json="{}",
        parent_uid=None,
    )
    span_other = SpanDbRow(
        uid=uuid.uuid4().bytes,
        name="span_other",
        data_json="{}",
        parent_uid=None,
    )
    tmp_db.insert_or_update(**span_1.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.uid]
    tmp_db.insert_or_update(**span_other.model_dump())
    assert tmp_db.get_span_ids_from_name(name="span") == [span_1.uid]
    tmp_db.insert_or_update(**span_2.model_dump())
    assert set(tmp_db.get_span_ids_from_name(name="span")) == set(
        [span_1.uid, span_2.uid]
    )


def test_get_last_span_uid(tmp_db: SpanDataBase) -> None:
    for i in range(5):
        span = SpanDbRow(
            uid=new_uid(),
            name=f"span_{i}",
            data_json="{}",
            parent_uid=None,
        )
        tmp_db.insert_or_update(**span.model_dump())
        assert tmp_db.get_last_span_uid() == span.uid


def test_get_last_updated_span_uid(tmp_db: SpanDataBase) -> None:
    uids = []
    time_prev: float = 0.0
    # Test new
    for i in range(5):
        span = SpanDbRow(
            uid=new_uid(),
            name=f"span_{i}",
            data_json="{}",
            parent_uid=None,
        )
        uids.append(span.uid)
        tmp_db.insert_or_update(**span.model_dump())
        uid_updated_last, time_updated_last = tmp_db.get_last_updated_span_uid()
        assert uid_updated_last == span.uid
        assert time_updated_last > time_prev
        time_prev = time_updated_last
        time.sleep(0.001)
    # Test update data_json
    for i in reversed(range(5)):
        tmp_db.update_data_json(uid=uids[i], data_json=json.dumps(dict(test=i)))
        uid_updated_last, time_updated_last = tmp_db.get_last_updated_span_uid()
        assert uid_updated_last == uids[i]
        assert time_updated_last > time_prev
        time_prev = time_updated_last
        time.sleep(0.001)
