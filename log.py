
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s\n\n",
    handlers=[
        logging.FileHandler("pipeline_exec.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

def log_message(level, message):
    """
    Log a message at the specified level.

    Args:
        level (str): Log level ('info', 'error', 'warning', etc.).
        message (str): Message to log.
    """
    log_func = getattr(logging, level.lower(), None)
    if callable(log_func):
        log_func(message)