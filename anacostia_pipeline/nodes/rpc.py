from logging import Logger
from typing import List
import time

import httpx



class BaseRPC:
    def __init__(self, name: str, loggers: Logger | List[Logger] = None):
        self.name = name
        self.loggers = loggers
        self.root_node_url = None
    
    def set_root_node_url(self, root_node_url: str):
        self.root_node_url = root_node_url

    def log(self, message: str, level="DEBUG") -> None:
        if len(self.loggers) > 0:
            for logger in self.loggers:
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
            print(message)
    
    def setup(self):
        """
        Override to specify how to setup the RPC connection
        """
        pass

    def leaf_setup(self):
        self.setup()

        self.log(f"'{self.name}' waiting for root predecessors to connect", level='INFO')
        
        while self.root_node_url is None:
            time.sleep(0.1)
        
        self.log(f"'{self.name}' connected to root predecessors {self.root_node_url}", level='INFO')