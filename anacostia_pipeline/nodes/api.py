from logging import Logger
import httpx
from fastapi import FastAPI, status
from fastapi import HTTPException
from pydantic import BaseModel
from typing import List, Union
import asyncio
import threading



class RPCConnectionModel(BaseModel):
    url: str


class NetworkConnectionNotEstablished(Exception):
    """Raised when a network connection has not been established."""
    def __init__(self, message="Network connection has not been made yet."):
        super().__init__(message)


class BaseServer(FastAPI):
    """
    BaseServer is a FastAPI application that acts as a server to handle remote procedure calls from clients.
    Pipelines can use BaseServer to expose endpoints for clients on other pipelines to request data, models, metrics, and more.
    Pipelines can also use BaseServer to connect to clients on other pipelines.
    """
    
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

        if self.client_url is not None:
            if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
                # If no SSL certificates are provided, create a client without them
                self.client = httpx.AsyncClient(base_url=self.client_url)
                self.scheme = "http"
            else:
                # If SSL certificates are provided, use them to create the client
                try:
                    self.client = httpx.AsyncClient(
                        base_url=self.client_url, 
                        verify=self.ssl_ca_certs, 
                        cert=(self.ssl_certfile, self.ssl_keyfile)
                    )
                    self.scheme = "https"

                    # Validate that client_url is using HTTPS if SSL certificates are provided
                    if self.client_url:
                        parsed_url = httpx.URL(self.client_url)
                        if parsed_url.scheme != "https":
                            raise ValueError(f"Invalid client URL scheme: {self.client_url}. Must be 'https' when SSL certificates are provided.")
                    
                except httpx.ConnectError as e:
                    raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

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
    
    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the server. This is done to ensure the server uses the same event loop as the connector.
        """
        self.loop = loop

    def connect(self) -> None:
        """
        Connect to the client URL and register the server with the client.
        """

        async def _connect():
            if self.client_url is not None:
                response = await self.client.post("/api/client/connect", json={"url": self.get_server_url()})
                if response.status_code == 200:
                    message = response.json()["message"]
                    self.log(message, level="INFO")
                else:
                    raise HTTPException(status_code=response.status_code, detail=response.json().get("error", "Unknown error occurred"))
                return response
        
        if self.loop.is_running():
            response = asyncio.run_coroutine_threadsafe(_connect(), self.loop)
            return response.result()
        else:
            raise RuntimeError("Event loop is not running. Cannot connect to client.")

class BaseClient(FastAPI):
    """
    BaseClient is a FastAPI application that acts as a client to connect to a BaseServer.
    Pipelines can use BaseClient to connect to a server and perform remote procedure calls to retrieve data, models, metrics, and more.
    """

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

        self.loop: asyncio.AbstractEventLoop = None

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
                self.log(f"server '{server.url}' connected to client at '{self.get_client_url()}'", level="INFO")
                self.server_url = server.url
                self.setup_http_client()
                return {"message": f"client '{self.get_client_url()}' connected to server at '{server.url}'"}
        else:
            self.setup_http_client()
            self.log(f"Client '{self.get_client_url()}' initialized, connected to {self.server_url}", level="INFO")
            
            # Function to start and run the event loop in a separate thread
            def start_loop(loop: asyncio.AbstractEventLoop):
                asyncio.set_event_loop(loop)
                loop.run_forever()

            # Create and start the event loop thread
            self.loop = asyncio.new_event_loop()
            self.loop_thread = threading.Thread(target=start_loop, args=(self.loop,), daemon=True)

            # Note: when the client is given a server_url, we assume it is not being mounted into PipelineServer.
            # Instead, it is being run independently, so we create an event loop for it, and run it in a separate thread.
            # Therefore, all of the client methods that interact with the server will be submitted to this event loop.

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the client. This is done to ensure the client uses the same event loop as the server.
        """
        self.loop = loop

    def set_credentials(self, host: str, port: int, ssl_keyfile: str, ssl_certfile: str, ssl_ca_certs: str) -> None:
        self.host = host
        self.port = port
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        if self.server_url is not None:
            self.setup_http_client()

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
            raise NetworkConnectionNotEstablished(
                f"server_url = None, this is likely because the client '{self.client_name}' has not been connected to a server yet."
            )
        return self.server_url
    
    def get_client_url(self):
        # sample output: http://127.0.0.1:8000/metadata/api/client
        return f"{self.scheme}://{self.client_host}:{self.client_port}{self.get_client_prefix()}"
    
    def setup_http_client(self) -> None:
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient(base_url=self.server_url)

            # Validate that server_url is using HTTPS if SSL certificates are provided
            if self.server_url:
                parsed_url = httpx.URL(self.server_url)
                if parsed_url.scheme != "http":
                    raise ValueError(f"Invalid server URL scheme: {self.server_url}. Must be 'http' when SSL certificates are not provided.")
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(
                    base_url=self.server_url, 
                    verify=self.ssl_ca_certs, 
                    cert=(self.ssl_certfile, self.ssl_keyfile)
                )

                # Validate that server_url is using HTTPS if SSL certificates are provided
                if self.server_url:
                    parsed_url = httpx.URL(self.server_url)
                    if parsed_url.scheme != "https":
                        raise ValueError(f"Invalid server URL scheme: {self.server_url}. Must be 'https' when SSL certificates are provided.")

            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

    def start_client(self) -> None:
        """
        Start the client by starting its event loop in a separate thread.
        This is useful for running the client without a PipelineServer,
        allowing it to run independently and handle requests asynchronously.
        """

        try:
            self.loop_thread.start()
            self.log("Event loop started in a separate thread", level="INFO")

        except (KeyboardInterrupt, SystemExit):
            self.log("Event loop stopped by user", level="INFO")
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join()