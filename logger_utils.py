import logging

def setup_logger():
    """Return a configured logger."""
    logger = logging.getLogger("ETL_Pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    
    return logger