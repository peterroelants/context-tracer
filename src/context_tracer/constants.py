from typing import Final, Literal

# Common keys
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
EXCEPTION_KEY: Final[str] = "exception"
EXCEPTION_TYPE_KEY: Final[str] = "type"
EXCEPTION_VALUE_KEY: Final[str] = "value"
EXCEPTION_TRACEBACK_KEY: Final[str] = "traceback"
