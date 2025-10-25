import logging
import sys


def setup_logging():
    """
    Configures application-wide logging with both file and console output.
    Uses the standard 'process' attribute for process ID.
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
    """Return a named logger instance."""
    return logging.getLogger(name)
