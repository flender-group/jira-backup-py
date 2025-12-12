import logging

def get_logger(name: str = "backup", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(f"{name}.log")
        formatter = logging.Formatter(
            '%(asctime)s, %(levelname)s,%(module)s,%(filename)s:%(lineno)d,%(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
