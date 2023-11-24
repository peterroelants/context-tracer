from typing import Final, Literal

# Common keys
START_TIME_KEY = "start_time"
END_TIME_KEY = "end_time"
NAME_KEY: Final[Literal["name"]] = "name"
DATA_KEY: Final[Literal["data"]] = "data"

# Function decorator keys
FUNCTION_DECORATOR_KEY = "trace_function"
FUNCTION_NAME_KEY = "name"
FUNCTION_KWARGS_KEY = "kwargs"
FUNCTION_RETURNED_KEY = "returned"

# Exception keys
EXCEPTION_KEY = "exception"
EXCEPTION_TYPE_KEY = "type"
EXCEPTION_VALUE_KEY = "value"
EXCEPTION_TRACEBACK_KEY = "traceback"
