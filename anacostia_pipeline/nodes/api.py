from logging import Logger
import httpx
from fastapi import FastAPI, status
from pydantic import BaseModel
from typing import List, Union



class RPCConnectionModel(BaseModel):
    url: str


class NetworkConnectionNotEstablished(Exception):
    """Raised when a network connection has not been established."""
    def __init__(self, message="Network connection has not been made yet."):
        super().__init__(message)


# provides endpoints for client to call to execute remote procedure calls
# endpoints call methods on the node
class BaseServer(FastAPI):
    def __init__(
        self, 
        node, 
        client_url: str, 
        host: str = "127.0.0.1", 
        port: int = 8000, 
        loggers: Union[Logger, List[Logger]] = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.client_url = client_url
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs
        self.scheme = "https" if self.ssl_ca_certs and self.ssl_certfile and self.ssl_keyfile else "http"

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
        return f"/{self.node.name}/api/server"
    
    def get_server_url(self):
        # sample output: http://127.0.0.1:8000/metadata/api/server
        return f"{self.scheme}://{self.host}:{self.port}{self.get_node_prefix()}"
    
    async def connect(self, client: httpx.AsyncClient) -> None:
        if self.client_url is not None:
            response = await client.post(f"{self.client_url}/api/client/connect", json={"url": self.get_server_url()})
            message = response.json()["message"]
            self.log(message, level="INFO")

    def setup_http_client(self) -> None:
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient()
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))

                # Validate that client_url is using HTTPS if SSL certificates are provided
                if self.client_url:
                    parsed_url = httpx.URL(self.client_url)
                    if parsed_url.scheme != "https":
                        raise ValueError(f"Invalid client URL scheme: {self.client_url}. Must be 'https' when SSL certificates are provided.")
                
            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")



# sends a connection request to the server
# provides methods for pipeline to call to do a remote procedure call on the node attached to the server
class BaseClient(FastAPI):
    def __init__(
        self, 
        client_name: str, 
        client_host: str = "127.0.0.1", 
        client_port: int = 8000, 
        server_url: str = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        loggers: Union[Logger, List[Logger]] = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.client_host = client_host      # currently client_host and client_port are only used for logging
        self.client_port = client_port
        self.client_name = client_name

        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs
        self.scheme = "https" if self.ssl_ca_certs and self.ssl_certfile and self.ssl_keyfile else "http"

        # if you set server_url, you should not run the client as a FastAPI app using uvicorn. 
        # this capability is here to enable developers to directly communicate with a node server to do things like logging.
        # this capability is useful for use cases where developers want to log metrics from a deployment environment
        self.server_url = server_url

        if loggers is None:
            self.loggers: List[Logger] = list()
        else:
            if isinstance(loggers, Logger):
                self.loggers: List[Logger] = [loggers]
            else:
                self.loggers: List[Logger] = loggers

        if self.server_url is None:
            @self.post("/connect", status_code=status.HTTP_200_OK)
            async def connect(server: RPCConnectionModel):
                self.log(f"server '{server.url}' connected to client at 'http://{self.client_host}:{self.client_port}/{self.client_name}'", level="INFO")
                self.server_url = server.url
                return {"message": f"client 'http://{self.client_host}:{self.client_port}/{self.client_name}' connected to server at '{server.url}'"}
    
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
    
    def get_client_prefix(self):
        return f"/{self.client_name}/api/client"
    
    def get_server_url(self):
        if self.server_url is None:
            raise NetworkConnectionNotEstablished(f"server_url = None, this is likely due to client {self.client_name} has not been connected to a server yet.")
        return self.server_url
    
    def get_client_url(self):
        # sample output: http://127.0.0.1:8000/metadata/api/client
        return f"{self.scheme}://{self.client_host}:{self.client_port}{self.get_client_prefix()}"
    
    def setup_http_client(self) -> None:
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient()
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))

                # Validate that server_url is using HTTPS if SSL certificates are provided
                if self.server_url:
                    parsed_url = httpx.URL(self.server_url)
                    if parsed_url.scheme != "https":
                        raise ValueError(f"Invalid server URL scheme: {self.server_url}. Must be 'https' when SSL certificates are provided.")

            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")
