import json
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanClientAPI,
    SpanDict,
    SpanPayload,
    running_server,
)
from context_tracer.utils.id_utils import new_uid


@pytest.fixture
def tmp_db_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.sqlite"
        yield db_path


def test_span_payload() -> None:
    parent_uid = new_uid()
    data = {"test": "test"}
    name = "test123"
    span = SpanPayload.from_bytes_ids(
        name=name,
        data_json=json.dumps(data),
        parent_uid=parent_uid,
    )
    assert span.name == name
    assert span.data_json == json.dumps(data)
    assert span.parent_uid_bytes == parent_uid
    assert span.parent_uid == SpanPayload.uid_to_str(parent_uid)


def test_running_server(tmp_db_path: Path) -> None:
    span_dict: SpanDict = {
        "uid": new_uid(),
        "name": "test",
        "data": {"test": "test"},
        "parent_uid": None,
    }
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        # Create span
        client.put_new_span(**span_dict)
        # Get span
        same_span = client.get_span(uid=span_dict["uid"])
        assert same_span == span_dict


def test_span_client_api_patch_update_span(tmp_db_path: Path) -> None:
    data_orig = {"a": 1, "b": 2}
    span_dict: SpanDict = {
        "uid": new_uid(),
        "name": "test",
        "data": data_orig,
        "parent_uid": None,
    }
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        # Create span
        client.put_new_span(**span_dict)
        # Get span
        same_span = client.get_span(uid=span_dict["uid"])
        assert same_span == span_dict
        # Update span
        new_data = {"b": 3, "c": 4}
        client.patch_update_span(uid=span_dict["uid"], data=new_data)
        # Get span
        same_span = client.get_span(uid=span_dict["uid"])
        assert same_span["data"] == data_orig | new_data


def test_span_client_api_get_children_uids(tmp_db_path: Path) -> None:
    span_1_dict: SpanDict = {
        "uid": new_uid(),
        "name": "test_1",
        "data": {"test": "test"},
        "parent_uid": None,
    }
    span_2_dict: SpanDict = {
        "uid": new_uid(),
        "name": "test_2",
        "data": {"test": "test"},
        "parent_uid": span_1_dict["uid"],
    }
    span_3_dict: SpanDict = {
        "uid": new_uid(),
        "name": "test_3",
        "data": {"test": "test"},
        "parent_uid": span_1_dict["uid"],
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
        children = client.get_children_uids(uid=span_1_dict["uid"])
        assert set(children) == set([span_2_dict["uid"], span_3_dict["uid"]])
        # Get Children of span_2
        children = client.get_children_uids(uid=span_2_dict["uid"])
        assert children == []
        # Get Children of span_3
        children = client.get_children_uids(uid=span_3_dict["uid"])
