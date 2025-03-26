"""
Logging configuration for the beardantic package.
"""
import logging
import sys
from typing import Optional


def configure_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """
    Configure logging for the beardantic package.
    
    Args:
        level: Logging level (default: logging.INFO)
        format_string: Custom format string for log messages
        log_file: Path to log file (if None, logs to stderr only)
    """
    if format_string is None:
        format_string = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    
    handlers = []
    
    # Always add stderr handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(logging.Formatter(format_string))
    handlers.append(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(format_string))
        handlers.append(file_handler)
    
    # Configure root logger for the package
    logger = logging.getLogger("beardantic")
    logger.setLevel(level)
    
    # Remove any existing handlers and add our handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    for handler in handlers:
        logger.addHandler(handler)


# Get the logger for the current module
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for the specified module.
    
    Args:
        name: Name of the module
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f"beardantic.{name}")
