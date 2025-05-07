from logging import Logger
import httpx
from fastapi import FastAPI, status
from pydantic import BaseModel
from typing import List, Union



class RPCConnectionModel(BaseModel):
    url: str



# provides endpoints for caller to call to execute remote procedure calls
# endpoints call methods on the node
class BaseRPCCallee(FastAPI):
    def __init__(self, node, caller_url: str, host: str = "127.0.0.1", port: int = 8000, loggers: Union[Logger, List[Logger]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.caller_url = caller_url

        if loggers is None:
            self.loggers: List[Logger] = list()
        else:
            if isinstance(loggers, Logger):
                self.loggers: List[Logger] = [loggers]
            else:
                self.loggers: List[Logger] = loggers
        
    def add_loggers(self, loggers: Union[Logger, List[Logger]]) -> None:
        if isinstance(loggers, Logger):
            self.loggers.append(loggers)
        else:
            self.loggers.extend(loggers)

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

    def get_node_prefix(self):
        return f"/{self.node.name}/rpc/callee"
    
    def get_callee_url(self):
        # sample output: http://127.0.0.1:8000/metadata/rpc/callee
        return f"http://{self.host}:{self.port}{self.get_node_prefix()}"
    
    async def connect(self):
        if self.caller_url is not None:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.caller_url}/rpc/caller/connect", json={"url": self.get_callee_url()})
                message = response.json()["message"]
                self.log(message, level="INFO")



# sends a connection request to the server
# provides methods for pipeline to call to do a remote procedure call on the node attached to the callee
class BaseRPCCaller(FastAPI):
    def __init__(self, caller_name: str, caller_host: str = "127.0.0.1", caller_port: int = 8000, loggers: Union[Logger, List[Logger]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.caller_host = caller_host      # currently caller_host and caller_port are only used for logging
        self.caller_port = caller_port
        self.caller_name = caller_name
        self.callee_url = None

        if loggers is None:
            self.loggers: List[Logger] = list()
        else:
            if isinstance(loggers, Logger):
                self.loggers: List[Logger] = [loggers]
            else:
                self.loggers: List[Logger] = loggers

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(callee: RPCConnectionModel):
            self.log(f"Callee '{callee.url}' connected to caller at 'http://{self.caller_host}:{self.caller_port}/{self.caller_name}'", level="INFO")
            self.callee_url = callee.url
            return {"message": f"Caller 'http://{self.caller_host}:{self.caller_port}/{self.caller_name}' connected to callee at '{callee.url}'"}
    
    def add_loggers(self, loggers: Union[Logger, List[Logger]]) -> None:
        if isinstance(loggers, Logger):
            self.loggers.append(loggers)
        else:
            self.loggers.extend(loggers)

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
    
    def get_caller_prefix(self):
        return f"/{self.caller_name}/rpc/caller"
    
    def get_callee_url(self):
        return self.callee_url
    
    def get_caller_url(self):
        # sample output: http://127.0.0.1:8000/metadata/rpc/caller
        return f"http://{self.caller_host}:{self.caller_port}{self.get_caller_prefix()}"