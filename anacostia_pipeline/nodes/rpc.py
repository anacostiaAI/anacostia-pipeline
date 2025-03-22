from logging import Logger
from typing import List, Union

import httpx



class BaseRPC:
    def __init__(self, name: str, loggers: Union[Logger, List[Logger]] = None):
        self.name = name
        self.loggers = loggers
    
    def setup(self):
        """
        Override to specify how to setup the RPC connection
        """
        pass