import json
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanClientAPI,
    SpanDict,
    SpanPayload,
    running_server,
)


@pytest.fixture
def tmp_db_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.sqlite"
        yield db_path


def test_span_payload() -> None:
    parent_id = uuid.uuid1().bytes
    data = {"test": "test"}
    name = "test123"
    span = SpanPayload.from_bytes_ids(
        name=name,
        data_json=json.dumps(data),
        parent_id=parent_id,
    )
    assert span.name == name
    assert span.data_json == json.dumps(data)
    assert span.parent_id_bytes == parent_id
    assert span.parent_id == SpanPayload.id_to_str(parent_id)


def test_span_payload_id_conversion() -> None:
    for uid in [uuid.uuid1(), uuid.uuid4(), uuid.uuid5(uuid.uuid1(), "test")]:
        uid_bytes = uid.bytes
        uid_str = SpanPayload.id_to_str(uid_bytes)
        assert SpanPayload.id_to_bytes(uid_str) == uid_bytes
        assert SpanPayload.id_to_str(uid_bytes) == uid_str


def test_running_server(tmp_db_path: Path) -> None:
    span_dict: SpanDict = {
        "id": uuid.uuid1().bytes,
        "name": "test",
        "data": {"test": "test"},
        "parent_id": None,
    }
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        # Create span
        client.put_new_span(**span_dict)
        # Get span
        same_span = client.get_span(id=span_dict["id"])
        assert same_span == span_dict


def test_span_client_api_patch_update_span(tmp_db_path: Path) -> None:
    data_orig = {"a": 1, "b": 2}
    span_dict: SpanDict = {
        "id": uuid.uuid1().bytes,
        "name": "test",
        "data": data_orig,
        "parent_id": None,
    }
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        # Create span
        client.put_new_span(**span_dict)
        # Get span
        same_span = client.get_span(id=span_dict["id"])
        assert same_span == span_dict
        # Update span
        new_data = {"b": 3, "c": 4}
        client.patch_update_span(id=span_dict["id"], data=new_data)
        # Get span
        same_span = client.get_span(id=span_dict["id"])
        assert same_span["data"] == data_orig | new_data


def test_span_client_api_get_childrre(tmp_db_path: Path) -> None:
    span_1_dict: SpanDict = {
        "id": uuid.uuid1().bytes,
        "name": "test_1",
        "data": {"test": "test"},
        "parent_id": None,
    }
    span_2_dict: SpanDict = {
        "id": uuid.uuid1().bytes,
        "name": "test_2",
        "data": {"test": "test"},
        "parent_id": span_1_dict["id"],
    }
    span_3_dict: SpanDict = {
        "id": uuid.uuid1().bytes,
        "name": "test_3",
        "data": {"test": "test"},
        "parent_id": span_1_dict["id"],
    }
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        # Create spans
        client.put_new_span(**span_1_dict)
        client.put_new_span(**span_2_dict)
        client.put_new_span(**span_3_dict)
        # Get Children of span_1
        children = client.get_children_ids(id=span_1_dict["id"])
        assert set(children) == set([span_2_dict["id"], span_3_dict["id"]])
        # Get Children of span_2
        children = client.get_children_ids(id=span_2_dict["id"])
        assert children == []
        # Get Children of span_3
        children = client.get_children_ids(id=span_3_dict["id"])
