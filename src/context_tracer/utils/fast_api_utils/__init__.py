from fastapi import APIRouter, FastAPI, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .fast_api_process_runner import FastAPIProcessRunner


class StaticNoCache(StaticFiles):
    """Disable caching for static files."""

    def is_not_modified(self, *args, **kwargs) -> bool:
        return False

    def file_response(self, *args, **kwargs) -> Response:
        resp = super().file_response(*args, **kwargs)
        resp.headers["Cache-Control"] = "no-cache"
        return resp
