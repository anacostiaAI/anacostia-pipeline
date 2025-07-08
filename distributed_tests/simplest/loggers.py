root_test_path = "./testing_artifacts"
anacostia_log_path = f"{root_test_path}/anacostia.log"
access_log_path = f"{root_test_path}/access.log"

ROOT_ACCESS_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "ROOT ACCESS %(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "access_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": f"{access_log_path}",   # access_log_path = "./testing_artifacts/access.log" , log_path = "./testing_artifacts/anacostia.log"
        },
    },
    "loggers": {
        "uvicorn.access": {
            "handlers": ["access_file_handler"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

LEAF_ACCESS_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "LEAF ACCESS %(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "access_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": f"{access_log_path}",   # access_log_path = "./testing_artifacts/access.log" , log_path = "./testing_artifacts/anacostia.log"
        },
    },
    "loggers": {
        "uvicorn.access": {
            "handlers": ["access_file_handler"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

ROOT_ANACOSTIA_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "anacostia_fmt": {
            "format": "ROOT %(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "combined_fmt": {
            "format": "ROOT %(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "anacostia_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "anacostia_fmt",
            "filename": anacostia_log_path,
            "mode": "a",
        },
    },
    "loggers": {
        "root_anacostia": {
            "handlers": ["anacostia_file_handler"],
            "level": "INFO",
            "propagate": False,
        }
    }
}

LEAF_ANACOSTIA_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "anacostia_fmt": {
            "format": "LEAF %(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "combined_fmt": {
            "format": "LEAF %(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "anacostia_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "anacostia_fmt",
            "filename": anacostia_log_path,
            "mode": "a",
        }
    },
    "loggers": {
        "leaf_anacostia": {
            "handlers": ["anacostia_file_handler"],
            "level": "INFO",
            "propagate": False,
        }
    }
}
