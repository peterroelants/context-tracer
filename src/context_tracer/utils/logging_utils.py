import logging
from pathlib import Path

log = logging.getLogger(__name__)


def setup_logging(
    log_path: Path | None = None,
    log_level: int = logging.INFO,
) -> None:
    if log_path is not None:
        log_path = log_path.expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_path))
        file_handler.setFormatter(
            logging.Formatter(
                fmt="# %(levelname)s: %(asctime)s :: %(name)s.%(funcName)s:%(lineno)d\n  %(message)s"
            )
        )
        file_handler.setLevel(log_level)
        # Add file handler to root logger
        logging.getLogger().addHandler(file_handler)
        log.debug(f"Logging Setup at to {log_path}, {log_level=}")
