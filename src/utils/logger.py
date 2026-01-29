"""
Logging utility for ETL pipeline
Logs to both console and file
Windows-compatible version (no Unicode symbols)
"""

import logging
import os
from datetime import datetime


def setup_logger(name, log_dir='logs'):
    """
    Set up logger with file and console handlers
    
    Args:
        name: Logger name (usually module name)
        log_dir: Directory for log files
    
    Returns:
        logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # File handler (daily log file) - UTF-8 encoding
    log_filename = f"{log_dir}/etl_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler - use UTF-8 if possible, fallback to system encoding
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# Test the logger
if __name__ == "__main__":
    logger = setup_logger('test')
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    print(f"Log file created in: logs/etl_{datetime.now().strftime('%Y%m%d')}.log")
