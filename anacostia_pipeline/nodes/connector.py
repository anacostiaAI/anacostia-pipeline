import asyncio
from typing import List, Coroutine
from contextlib import asynccontextmanager
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

    async def signal_remote_predecessors(self, result: Result):
        if len(self.node.remote_predecessors) > 0:
            async with self.client_context() as client:
                tasks = []
                for predecessor_url in self.node.remote_predecessors:
                    node_model: NodeModel = self.node.model()
                    connection_mode = NodeConnectionModel(
                        **node_model.model_dump(),
                        node_url=f"http://{self.host}:{self.port}/{self.node.name}",
                    )
                    json = connection_mode.model_dump()
                    tasks.append(
                        client.post(
                            # sample output: http://localhost:8000/metadata/connector/backward_signal
                            f"{predecessor_url}/connector/backward_signal", 
                            json=json
                        )
                    )

                await asyncio.gather(*tasks)

    async def signal_remote_successors(self, result: Result):
        if len(self.node.remote_successors) > 0:
            async with self.client_context() as client:
                tasks = []
                for successor_url in self.node.remote_successors:
                    node_model: NodeModel = self.node.model()
                    connection_mode = NodeConnectionModel(
                        **node_model.model_dump(),
                        node_url=f"http://{self.host}:{self.port}/{self.node.name}",
                    )
                    json = connection_mode.model_dump()
                    tasks.append(
                        client.post(
                            # sample output: http://localhost:8000/metadata/connector/forward_signal
                            f"{successor_url}/connector/forward_signal", 
                            json=json
                        )
                    )
                
                await asyncio.gather(*tasks)

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