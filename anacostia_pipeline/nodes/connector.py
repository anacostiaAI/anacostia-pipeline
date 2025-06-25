from typing import List, Coroutine
from contextlib import asynccontextmanager
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

        self._client = httpx.AsyncClient(
            verify = ssl_ca_certs if ssl_ca_certs is not None else True,
            cert = (ssl_certfile, ssl_keyfile) if ssl_certfile and ssl_keyfile else None
        )

        self.scheme = "https" if ssl_certfile and ssl_keyfile else "http"

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
    
    @asynccontextmanager
    async def client_context(self):
        """
        Yields the persistent client in a context-managed way.
        """
        try:
            yield self._client
        finally:
            pass  # Do not close here — close it manually on shutdown

    def get_connector_prefix(self):
        # sample output: /metadata/connector
        return f"/{self.node.name}/connector"
    
    def get_connect_url(self):
        # sample output: http://localhost:8000/metadata/connector/connect
        return f"{self.scheme}://{self.host}:{self.port}{self.get_connector_prefix()}/connect"

    def send_forward_signal(self):
        # sample output: http://localhost:8000/metadata/connector/forward_signal
        return f"{self.scheme}://{self.host}:{self.port}{self.get_connector_prefix()}/forward_signal"
    
    def send_backward_signal(self):
        # sample output: http://localhost:8000/metadata/connector/backward_signal
        return f"{self.scheme}://{self.host}:{self.port}{self.get_connector_prefix()}/backward_signal"
    
    async def connect(self) -> List[Coroutine]:

        async def connect_to_successor(connection_url: str, json: dict):
            async with self.client_context() as client:
                return await client.post(connection_url, json=json)

        tasks = []
        for connection in self.node.remote_successors:
            node_model: NodeModel = self.node.model()
            connection_mode = NodeConnectionModel(
                **node_model.model_dump(),
                node_url=f"http://{self.host}:{self.port}/{self.node.name}"
            )
            json = connection_mode.model_dump()
            tasks.append(
                connect_to_successor(connection_url=f"{connection}/connector/connect", json=json)
            )
        return tasks