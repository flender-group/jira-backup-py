import logging

def get_logger(name: str = "backup", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(f"../logs/{name}.log",encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s,%(levelname)s,%(funcName)s,%(filename)s,%(lineno)d,%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S%z'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
