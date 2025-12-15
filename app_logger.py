import logging

def get_logger(name: str = "backup", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(f"{name}.log")
        formatter = logging.Formatter(
            '{"TimeGenerated": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "file": "%(filename)s", "line": %(lineno)d, "message": "%(message)s"}',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
