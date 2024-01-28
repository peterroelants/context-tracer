# Context Tracer

Package to help trace Python function call contexts. Initially developed to trace LLM Agent function calls.


## Usage

To annotate a function to be traced, use the `@trace` decorator:
```python
from context_tracer.trace import trace

@trace
def demo_function():
    ...
```

To run a function or program with tracing enabled, use a `Tracing` context manager. For example the `TracingWithViewer` will open a web browser to show a flame chart of the traced function calls:
```python
from context_tracer.tracing_viewer.tracer_with_view import TracingWithViewer

with TracingWithViewer(
    db_path=...,
    ...
) as tracing:
    demo_function()
```
Other `Tracing` context managers are available, see [src/context_tracer/trace_implementations](src/context_tracer/trace_implementations) for more details.

### Full example

See [notebooks/example.ipynb](notebooks/example.ipynb) for a usage example.



## OTEL
This library has some similarities with [OTel (OpenTelemetry)](https://opentelemetry.io/docs/instrumentation/python/). OTEL is focussed on distributed tracing for microservice applications. This library is focussed on tracing function calls within a single application. OTEL is a much more mature project and has a lot more features. This library is much simpler and has a much smaller footprint.

### Questions/TODO:
- Follow OTel (OpenTelemetry) conventions for tracing to be more compatible with OTel.
  - https://opentelemetry.io/docs/specs/otel/trace/api/
  - https://opentelemetry.io/docs/specs/otel/overview/
  - https://opentelemetry.io/docs/concepts/signals/traces/
- Can this library be re-creating by building on top of OTel?
  - Using [manual instrumentation](https://opentelemetry.io/docs/instrumentation/python/manual/)?
  - Using a custom [collector](https://opentelemetry.io/docs/collector/)?
- Build building this library on top of Otel cause unnecessary overhead/conflicts?
  - E.g. The goal is different, you might want to separately trace function calls, and microservice telemetry.
- DAG visualisation:
  -  https://reactflow.dev/
  -  https://github.com/clientIO/joint
  -  https://js.cytoscape.org/
  -  https://github.com/dagrejs/dagre-d3/wiki
