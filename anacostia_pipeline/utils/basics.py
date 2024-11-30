from logging import Logger



# move log method into utils.py
def log(logger: Logger, message: str, level: str = "INFO"):
    if logger is not None:
        if level == "DEBUG":
            logger.debug(message)
        elif level == "INFO":
            logger.info(message)
        elif level == "WARNING":
            logger.warning(message)
        elif level == "ERROR":
            logger.error(message)
        elif level == "CRITICAL":
            logger.critical(message)
        else:
            raise ValueError(f"Invalid log level: {level}")
    else:
        print(f"{level}: {message}")