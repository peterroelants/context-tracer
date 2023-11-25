import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanClientAPI,
    running_server,
)


@pytest.fixture
def tmp_db_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.db"
        yield db_path


@pytest.fixture
def tmp_api_client(tmp_db_path: Path) -> Iterator[SpanClientAPI]:
    with running_server(db_path=tmp_db_path) as server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        yield client
