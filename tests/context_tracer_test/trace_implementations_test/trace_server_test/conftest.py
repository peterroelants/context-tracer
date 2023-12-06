from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanClientAPI,
    create_span_server,
)


@pytest.fixture
def tmp_api_client(tmp_db_path: Path) -> Iterator[SpanClientAPI]:
    server = create_span_server(db_path=tmp_db_path)
    with server:
        url = f"http://localhost:{server.port}"
        client = SpanClientAPI(url=url)
        client.wait_for_ready()
        assert client.is_ready()
        yield client
