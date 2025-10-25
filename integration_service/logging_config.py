"""
logging_config.py — Centralized Logging Configuration for Integration Service

This module configures unified logging behavior for the entire application.
It ensures that all modules log messages consistently to both console and file.

Features:
    • Combined console and file logging output
    • Process ID tagging for multi-process visibility
    • Standardized log format for all modules
    • Reduced verbosity for external dependencies (e.g., pika)
"""

import logging
import sys


def setup_logging():
    """
    Configures the global logging system for the application.

    The configuration includes:
        - Log level: INFO (default)
        - Log format: timestamp, log level, process ID, and message
        - Output destinations:
            1. File: 'order_processing.log' (persistent log)
            2. Console (stdout): real-time logs, Docker/Kubernetes compatible
        - Reduced verbosity for third-party libraries such as pika
    """
    log_format = '%(asctime)s - %(levelname)s - [PID:%(process)d] - %(message)s'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            # File output
            logging.FileHandler("order_processing.log"),
            # Console output (stdout, Docker-compatible)
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Reduce verbosity from external libraries
    logging.getLogger("pika").setLevel(logging.WARNING)


def get_logger(name):
    """
    Returns a configured logger instance for a given module or component name.

    Args:
        name (str): The logger name, typically the module’s __name__.

    Returns:
        logging.Logger: A preconfigured logger that follows the global format and handlers.

    Notes:
        - This function should be used instead of directly calling `logging.getLogger()`
          to maintain consistent naming conventions and global configuration.
    """
    return logging.getLogger(name)
