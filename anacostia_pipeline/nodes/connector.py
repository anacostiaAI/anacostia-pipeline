import asyncio
from urllib.parse import urlparse
from typing import List, Coroutine

import httpx
from fastapi import FastAPI, status
from anacostia_pipeline.nodes.utils import NodeConnectionModel, NodeModel



class Connector(FastAPI):
    def __init__(
        self, 
        node, 
        host: str, 
        port: int,
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certs = ssl_ca_certs

        # If SSL certificates are provided, use them to create the client
        self.client: httpx.AsyncClient = None

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: NodeConnectionModel) -> NodeConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            node_model: NodeModel = self.node.model()
            return NodeConnectionModel(
                **node_model.model_dump(),
                node_url=f"http://{self.host}:{self.port}{self.get_connector_prefix()}", 
            )
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: NodeConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: NodeConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def get_connector_prefix(self):
        # sample output: /metadata/connector
        return f"/{self.node.name}/connector"
    
    def get_node_url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}/{self.node.name}"

    def get_connect_url(self):
        # sample output: http://localhost:8000/metadata/connector/connect
        return f"{self.scheme}://{self.host}:{self.port}{self.get_connector_prefix()}/connect"

    async def close_client(self) -> None:
        await self.client.aclose()
    
    def setup_client(self) -> None:
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

    async def connect(self, client: httpx.AsyncClient) -> List[Coroutine]:
        """
        Connect to all remote predecessors and successors.
        Returns a list of coroutines that can be awaited to perform the connection.
        """
        task = []
        for connection in self.node.remote_successors:
            node_model: NodeModel = self.node.model()
            connection_mode = NodeConnectionModel(
                **node_model.model_dump(),
                node_url=self.get_node_url(),
            )
            json = connection_mode.model_dump()
            task.append(client.post(f"{connection}/connector/connect", json=json))

        await asyncio.gather(*task)
    
    async def signal_remote_predecessors(self):
        if len(self.node.remote_predecessors) > 0:
            tasks = []
            for predecessor_url in self.node.remote_predecessors:
                node_model: NodeModel = self.node.model()
                connection_mode = NodeConnectionModel(
                    **node_model.model_dump(),
                    node_url=self.get_node_url(),
                )
                json = connection_mode.model_dump()
                tasks.append(self.client.post(f"{predecessor_url}/connector/backward_signal", json=json))

            await asyncio.gather(*tasks)
    
    async def signal_remote_successors(self):
        if len(self.node.remote_successors) > 0:
            tasks = []
            for successor_url in self.node.remote_successors:
                node_model: NodeModel = self.node.model()
                connection_mode = NodeConnectionModel(
                    **node_model.model_dump(),
                    node_url=self.get_node_url(),
                )
                json = connection_mode.model_dump()
                tasks.append(self.client.post(f"{successor_url}/connector/forward_signal", json=json))

            await asyncio.gather(*tasks)