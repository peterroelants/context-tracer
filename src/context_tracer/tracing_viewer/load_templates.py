import html
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from jinja2 import BaseLoader, Environment

from context_tracer.utils.json_encoder import CustomEncoder
from context_tracer.utils.url_utils import (
    urljoin_forward_slash,
)

log = logging.getLogger(__name__)

# Resources
THIS_DIR = Path(__file__).parent.resolve()
# Templates
TEMPLATE_DIR = THIS_DIR / "templates"
assert TEMPLATE_DIR.exists() and TEMPLATE_DIR.is_dir()
HTML_TEMPLATE = TEMPLATE_DIR / "flamechart_view.html.jinja"
assert (
    HTML_TEMPLATE.exists() and HTML_TEMPLATE.is_file()
), f"'{HTML_TEMPLATE}' does not exist or is not a file."
# Static resources
STATIC_DIR = THIS_DIR / "server_static"
assert STATIC_DIR.exists() and STATIC_DIR.is_dir()
FLAME_CHART_STYLE_CSS_PATH = STATIC_DIR / "flame_chart_style.css"
assert FLAME_CHART_STYLE_CSS_PATH.exists() and FLAME_CHART_STYLE_CSS_PATH.is_file()
FLAME_CHART_VIEW_JS_PATH = STATIC_DIR / "flame_chart_view.js"
assert FLAME_CHART_VIEW_JS_PATH.exists() and FLAME_CHART_VIEW_JS_PATH.is_file()
JSONVIEW_JS_PATH = STATIC_DIR / "jsonview.js"
assert JSONVIEW_JS_PATH.exists() and JSONVIEW_JS_PATH.is_file()


def get_flamechart_view(
    data_dict: dict[str, Any] | None,
    css_js_static_path: str | None = None,
    websocket_url: str | None = None,
) -> str:
    """
    Get the html string for the flamechart view.
    """
    if websocket_url is not None:
        assert urlparse(websocket_url)
    # Parse the data into an html safe json string
    if not data_dict and websocket_url is None:
        log.warning(
            "No data provided, and no websocket url provided. Returning empty flame-chart that cannot be updated."
        )
    if data_dict:
        data_json = json.dumps(data_dict, cls=CustomEncoder, indent=2)
        data_json = html.escape(data_json, quote=False)
    else:
        data_json = None
    # Get custom css and js
    if css_js_static_path is None:
        # Inline source
        custom_css = create_css_inline(FLAME_CHART_STYLE_CSS_PATH)
        custom_js = "\n".join(
            [
                create_js_inline(FLAME_CHART_VIEW_JS_PATH),
                create_js_inline(JSONVIEW_JS_PATH),
            ]
        )
    else:
        # Use references
        custom_css = create_css_href(
            urljoin_forward_slash(css_js_static_path, FLAME_CHART_STYLE_CSS_PATH.name)
        )
        custom_js = "\n".join(
            [
                create_js_src(
                    urljoin_forward_slash(
                        css_js_static_path, FLAME_CHART_VIEW_JS_PATH.name
                    )
                ),
                create_js_src(
                    urljoin_forward_slash(css_js_static_path, JSONVIEW_JS_PATH.name)
                ),
            ]
        )
    # Render the template
    flamechart_html_str = (
        Environment(loader=BaseLoader())
        .from_string(HTML_TEMPLATE.read_text())
        .render(
            custom_css=custom_css,
            custom_js=custom_js,
            websocket_url=websocket_url,
            data_json=data_json,
        )
    )
    return flamechart_html_str


def create_css_href(href: str) -> str:
    return f'<link rel="stylesheet" type="text/css" href="{href}"/>'


def create_js_src(src: str) -> str:
    return f'<script type="text/javascript" src="{src}"></script>'


def create_css_inline(source: Path) -> str:
    return f'<style type="text/css">{source.read_text()}</style>'


def create_js_inline(source: Path) -> str:
    return f'<script type="text/javascript">{source.read_text()}</script>'
