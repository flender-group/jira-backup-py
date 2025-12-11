import logging

def get_logger(name: str = "backup") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(f"/var/log/{name}.log")
        formatter = logging.Formatter(
            '%(asctime)s, %(levelname)-8s,%(module)s,%(filename)s:%(lineno)d,%(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
