from typing import Final, Literal

# Common keys
TRACE_METADATA_KEY: Final[str] = "trace_metadata"
START_TIME_KEY: Final[str] = "start_time"
END_TIME_KEY: Final[str] = "end_time"
NAME_KEY: Literal["name"] = "name"
DATA_KEY: Literal["data"] = "data"

# Function decorator keys
FUNCTION_DECORATOR_KEY: Final[str] = "trace_function"
FUNCTION_NAME_KEY: Final[str] = "name"
FUNCTION_KWARGS_KEY: Final[str] = "kwargs"
FUNCTION_RETURNED_KEY: Final[str] = "returned"

# Exception keys
# Follow OTel convention as much as possible
# https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/#attributes
EXCEPTION_KEY: Final[str] = "exception"
EXCEPTION_TYPE_KEY: Final[str] = "type"
EXCEPTION_MESSAGE_KEY: Final[str] = "message"
EXCEPTION_STACKTRACE_KEY: Final[str] = "stacktrace"
