import asyncio
from urllib.parse import urlparse
from typing import List, Coroutine, Union
from logging import Logger

import httpx
from fastapi import FastAPI, status
from anacostia_pipeline.nodes.utils import NodeConnectionModel, NodeModel
from anacostia_pipeline.utils.constants import Result



class Connector(FastAPI):
    def __init__(
        self, 
        node, 
        host: str, 
        port: int,
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        loggers: Union[Logger, List[Logger]] = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        # Note: the client will be bound to the PipelineServer's event loop;
        # this happens when the Connector is initialized when PipelineServer call node.setup_connector()
        if self.ssl_ca_certs is None or self.ssl_certfile is None or self.ssl_keyfile is None:
            # If no SSL certificates are provided, create a client without them
            self.client = httpx.AsyncClient()
            self.scheme = "http"
        else:
            # If SSL certificates are provided, use them to create the client
            try:
                self.client = httpx.AsyncClient(verify=self.ssl_ca_certs, cert=(self.ssl_certfile, self.ssl_keyfile))
                self.scheme = "https"

                for predecessor_url in self.node.remote_predecessors:
                    parsed_url = urlparse(predecessor_url)
                    if parsed_url.scheme != "https":
                        raise ValueError(f"Invalid URL scheme for remote predecessor: {predecessor_url}. Must be 'https'.")

                for successor_url in self.node.remote_successors:
                    parsed_url = urlparse(successor_url)
                    if parsed_url.scheme != "https":
                        raise ValueError(f"Invalid URL scheme for remote successor: {successor_url}. Must be 'https'.")

            except httpx.ConnectError as e:
                raise ValueError(f"Failed to create HTTP client with SSL certificates: {e}")

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: NodeConnectionModel) -> NodeConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            node_model: NodeModel = self.node.model()
            return NodeConnectionModel(
                **node_model.model_dump(),
                node_url=f"{self.scheme}://{self.host}:{self.port}{self.get_connector_prefix()}", 
            )
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: NodeConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled successors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: NodeConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
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

    def get_connector_prefix(self):
        # sample output: /metadata/connector
        return f"/{self.node.name}/connector"
    
    def get_node_url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}/{self.node.name}"

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Set the event loop for the connector. This is done to ensure the connector uses the same event loop as the server.
        """
        self.loop = loop

    def connect(self) -> List[Coroutine]:
        """
        Connect to all remote predecessors and successors.
        Returns a list of coroutines that can be awaited to perform the connection.
        """

        async def _connect():
            tasks = []
            for connection in self.node.remote_successors:
                node_model: NodeModel = self.node.model()
                connection_mode = NodeConnectionModel(
                    **node_model.model_dump(),
                    node_url=self.get_node_url(),
                )
                json = connection_mode.model_dump()
                tasks.append(
                    self.client.post(f"{connection}/connector/connect", json=json)
                )
            responses = await asyncio.gather(*tasks)
            return responses
 
        if len(self.node.remote_successors) > 0:
            if self.loop.is_running():
                response = asyncio.run_coroutine_threadsafe(_connect(), self.loop)
                results = response.result()
                results = [r.json() for r in results if r.status_code == 200]
                self.log(f"Node '{self.node.name}' connected to remote successors: {[r['node_url'] for r in results]}", level="INFO")
                return results
            else:
                raise RuntimeError("Event loop is not running. Cannot connect to remote successors.")

    def signal_remote_predecessors(self) -> List[Coroutine]:
        """
        Signal all remote predecessors that the node has finished processing.
        Returns a list of coroutines that can be awaited to perform the signaling.
        """

        async def _signal_remote_predecessors():
            tasks = []
            for predecessor_url in self.node.remote_predecessors:
                node_model: NodeModel = self.node.model()
                connection_mode = NodeConnectionModel(
                    **node_model.model_dump(),
                    node_url=self.get_node_url(),
                )
                json = connection_mode.model_dump()
                tasks.append(self.client.post(f"{predecessor_url}/connector/backward_signal", json=json))

            responses = await asyncio.gather(*tasks)
            return responses
        
        if len(self.node.remote_predecessors) > 0:
            if self.loop.is_running():
                response = asyncio.run_coroutine_threadsafe(_signal_remote_predecessors(), self.loop)
                return response.result()
            else:
                raise RuntimeError("Event loop is not running. Cannot signal remote predecessors.")

    def signal_remote_successors(self) -> List[Coroutine]:
        """
        Signal all remote successors that the node has finished processing.
        Returns a list of coroutines that can be awaited to perform the signaling.
        """

        async def _signal_remote_successors():
            tasks = []
            for successor_url in self.node.remote_successors:
                node_model: NodeModel = self.node.model()
                connection_mode = NodeConnectionModel(
                    **node_model.model_dump(),
                    node_url=self.get_node_url(),
                )
                json = connection_mode.model_dump()
                tasks.append(self.client.post(f"{successor_url}/connector/forward_signal", json=json))

            responses = await asyncio.gather(*tasks)
            return responses

        if len(self.node.remote_successors) > 0:
            if self.loop.is_running():
                response = asyncio.run_coroutine_threadsafe(_signal_remote_successors(), self.loop)
                return response.result()
            else:
                raise RuntimeError("Event loop is not running. Cannot signal remote successors.")
