import logging
from rich.logging import RichHandler

def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_time=False)],
    )