import logging 
import sys
def get_logger (name: str = "Generic Logger", message: str = "Logger initialized"):
    """Returns a logger with the given name and logs the initialization message once."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Prevent adding multiple handlers if the logger already has one
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    logger.info(message)
    return logger
