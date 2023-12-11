import contextlib
import functools
import json
import logging
import os
import time
from http import HTTPStatus
from pathlib import Path
from typing import Any, AsyncIterator, Final, Self, TypedDict

import requests
from fastapi import APIRouter, FastAPI
from fastapi.responses import Response
from pydantic import BaseModel

from context_tracer.trace_implementations.trace_sqlite.span_db import (
    SpanDataBase,
)
from context_tracer.utils.fast_api_utils import (
    FastAPIProcessRunner,
)
from context_tracer.utils.fast_api_utils.readiness import (
    READINESS_ENDPOINT_PATH,
    readiness_api,
)
from context_tracer.utils.id_utils import uid_to_bytes, uid_to_str
from context_tracer.utils.json_encoder import CustomEncoder
from context_tracer.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)


SPAN_ENDPOINT_PATH: Final[str] = "/api/span/{span_uid}"
SPAN_CHILDREN_ENDPOINT_PATH: Final[str] = "/api/span/{span_uid}/children"
ROOT_SPAN_IDS_ENDPOINT_PATH: Final[str] = "/api/tracing/root"


# Types ############################################################
class SpanDict(TypedDict):
    uid: bytes
    name: str
    data: dict[str, Any]
    parent_uid: bytes | None


class SpanPayloadBytesDict(TypedDict):
    name: str
    data_json: str
    parent_uid: bytes | None


class SpanPayloadDict(TypedDict):
    name: str
    data_json: str
    parent_uid: str | None


class SpanDataPayload(BaseModel):
    data_json: str


class SpanPayload(SpanDataPayload):
    """Request posted to the trace server."""

    name: str
    parent_uid: str | None

    @property
    def parent_uid_bytes(self) -> bytes | None:
        return self.maybe_uid_to_bytes(self.parent_uid)

    @classmethod
    def from_bytes_ids(
        cls: type[Self],
        name: str,
        data_json: str,
        parent_uid: bytes | None,
        **kwargs,
    ) -> Self:
        parent_uid_str = cls.maybe_uid_to_str(parent_uid)
        return cls(
            name=name,
            data_json=data_json,
            parent_uid=parent_uid_str,
        )

    @staticmethod
    def uid_to_bytes(uid: str) -> bytes:
        return uid_to_bytes(uid)

    @staticmethod
    def maybe_uid_to_bytes(uid: str | None) -> bytes | None:
        if uid is None:
            return None
        return SpanPayload.uid_to_bytes(uid)

    @staticmethod
    def uid_to_str(id: bytes) -> str:
        return uid_to_str(id)

    @staticmethod
    def maybe_uid_to_str(uid: bytes | None) -> str | None:
        if uid is None:
            return None
        return SpanPayload.uid_to_str(uid)

    def model_dump_byte_ids(self) -> SpanPayloadBytesDict:
        return {
            "name": self.name,
            "data_json": self.data_json,
            "parent_uid": self.parent_uid_bytes,
        }


# Client ###########################################################
class SpanClientAPI:
    """Client API for Span server."""

    url: str

    def __init__(self, url: str) -> None:
        self.url = url

    def wait_for_ready(
        self,
        timeout_sec: float = 30,
        poll_interval_sec: float = 0.5,
    ) -> None:
        """Wait for the server to be ready."""
        start_time = time.time()
        while not self.is_ready():
            time.sleep(poll_interval_sec)
            if time.time() - start_time > timeout_sec:
                raise TimeoutError("Timed out waiting for server to be ready.")

    def is_ready(self) -> bool:
        resp = requests.get(f"{self.url}{READINESS_ENDPOINT_PATH}")
        return resp.status_code == HTTPStatus.OK

    def get_span(self, uid: bytes) -> SpanDict:
        span_uid = SpanPayload.uid_to_str(uid)
        resp = requests.get(f"{self.url}{SPAN_ENDPOINT_PATH.format(span_uid=span_uid)}")
        resp.raise_for_status()
        resp_json: SpanPayloadDict = resp.json()
        span_dict: SpanDict = dict(
            uid=uid,
            name=resp_json["name"],
            data=json.loads(resp_json["data_json"]),
            parent_uid=SpanPayload.maybe_uid_to_bytes(resp_json["parent_uid"]),
        )
        return span_dict

    def put_new_span(
        self,
        uid: bytes,
        name: str,
        data: dict[str, Any],
        parent_uid: bytes | None = None,
    ) -> None:
        span_uid: str = SpanPayload.uid_to_str(uid)
        request_payload: SpanPayload = SpanPayload.from_bytes_ids(
            name=name,
            data_json=json.dumps(data, cls=CustomEncoder),
            parent_uid=parent_uid,
        )
        resp = requests.put(
            f"{self.url}{SPAN_ENDPOINT_PATH.format(span_uid=span_uid)}",
            json=request_payload.model_dump(),
        )
        resp.raise_for_status()

    def patch_update_span(self, uid: bytes, data: dict[str, Any]) -> None:
        span_uid = SpanPayload.uid_to_str(uid)
        request_payload = SpanDataPayload(data_json=json.dumps(data, cls=CustomEncoder))
        resp = requests.patch(
            f"{self.url}{SPAN_ENDPOINT_PATH.format(span_uid=span_uid)}",
            json=request_payload.model_dump(),
        )
        resp.raise_for_status()

    def get_children_uids(self, uid: bytes) -> list[bytes]:
        span_uid = SpanPayload.uid_to_str(uid)
        resp = requests.get(
            f"{self.url}{SPAN_CHILDREN_ENDPOINT_PATH.format(span_uid=span_uid)}"
        )
        resp.raise_for_status()
        return [SpanPayload.uid_to_bytes(child_uid) for child_uid in resp.json()]

    def get_root_span_ids(self) -> list[bytes]:
        resp = requests.get(f"{self.url}{ROOT_SPAN_IDS_ENDPOINT_PATH}")
        resp.raise_for_status()
        return [SpanPayload.uid_to_bytes(uid) for uid in resp.json()]


# Server ###########################################################
class SpanServerAPI:
    """
    API for Span server.
    Provides POST endpoints to update the span database.
    """

    span_db: SpanDataBase

    def __init__(self, span_db: SpanDataBase) -> None:
        self.span_db = span_db

    async def get_span(self, span_uid: str) -> SpanPayload:
        span = self.span_db.get_span(uid=SpanPayload.uid_to_bytes(span_uid))
        span_response = SpanPayload.from_bytes_ids(**span.model_dump())
        return span_response

    async def put_new_span(self, span_uid: str, span: SpanPayload):
        self.span_db.insert(
            uid=SpanPayload.uid_to_bytes(span_uid), **span.model_dump_byte_ids()
        )
        return Response(status_code=HTTPStatus.OK)

    async def patch_update_span(self, span_uid: str, span_data: SpanDataPayload):
        self.span_db.update_data_json(
            uid=SpanPayload.uid_to_bytes(span_uid), data_json=span_data.data_json
        )
        return Response(status_code=HTTPStatus.OK)

    async def get_children_ids(self, span_uid: str) -> list[str]:
        child_uids: list[bytes] = self.span_db.get_children_uids(
            uid=SpanPayload.uid_to_bytes(span_uid)
        )
        return [SpanPayload.uid_to_str(uid) for uid in child_uids]

    async def get_root_span_ids(self) -> list[str]:
        uids: list[bytes] = self.span_db.get_root_uids()
        return [SpanPayload.uid_to_str(uid) for uid in uids]

    @contextlib.asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        """
        ASGI lifespan event handler.

        - https://fastapi.tiangolo.com/advanced/events/
        - https://asgi.readthedocs.io/en/latest/specs/lifespan.html
        """
        log.debug(f"{self.__class__.__name__}.lifespan enter")
        yield
        log.debug(f"{self.__class__.__name__}.lifespan exit")

    def get_router(
        self,
        span_api_path: str = SPAN_ENDPOINT_PATH,
        span_children_path: str = SPAN_CHILDREN_ENDPOINT_PATH,
        span_root_ids_path: str = ROOT_SPAN_IDS_ENDPOINT_PATH,
    ) -> APIRouter:
        """
        Returns a router with the span server HTTP API endpoints.
        Database backed server.
        To be included in a FastAPI app.
        """
        router = APIRouter()
        router.add_api_route(span_api_path, self.put_new_span, methods=["PUT"])
        router.add_api_route(span_api_path, self.patch_update_span, methods=["PATCH"])
        router.add_api_route(
            span_api_path, self.get_span, methods=["GET"], response_model=SpanPayload
        )
        router.add_api_route(span_children_path, self.get_children_ids, methods=["GET"])
        router.add_api_route(
            span_root_ids_path, self.get_root_span_ids, methods=["GET"]
        )
        return router

    @classmethod
    def create_app(
        cls,
        # Host and port are passed by the FastAPIProcessRunner
        host: str,
        port: int,
        span_db: SpanDataBase,
        span_api_path: str = SPAN_ENDPOINT_PATH,
        readiness_path: str = READINESS_ENDPOINT_PATH,
        span_children_path: str = SPAN_CHILDREN_ENDPOINT_PATH,
        span_root_ids_path: str = ROOT_SPAN_IDS_ENDPOINT_PATH,
        log_path: Path | None = None,
        log_level: int = logging.INFO,
    ) -> FastAPI:
        """
        Returns a FastAPI app with the span server HTTP API endpoints.
        Database backed server.
        """
        setup_logging(log_path=log_path, log_level=log_level)
        log.debug(f"{cls.__name__}.create_app()")
        api = cls(span_db=span_db)
        router = api.get_router(
            span_api_path=span_api_path,
            span_children_path=span_children_path,
            span_root_ids_path=span_root_ids_path,
        )
        app = FastAPI(lifespan=api.lifespan)
        app.add_api_route(readiness_path, readiness_api, methods=["GET"])
        app.include_router(router)
        log.debug(f"{cls.__name__} FastAPI app created: {app=!r} on PID={os.getpid()}")
        return app


def create_span_server(
    db_path: Path,
    log_path: Path | None = None,
    **server_kwargs,
) -> FastAPIProcessRunner:
    log.info(f"Create remote trace server with db_path='{db_path!s}'")
    log_level = server_kwargs.pop("log_level", logging.INFO)
    span_db = SpanDataBase(db_path=db_path)
    create_app = functools.partial(
        SpanServerAPI.create_app,
        span_db=span_db,
        span_api_path=SPAN_ENDPOINT_PATH,
        log_level=log_level,
        log_path=log_path,
    )
    return FastAPIProcessRunner(create_app=create_app, **server_kwargs)
