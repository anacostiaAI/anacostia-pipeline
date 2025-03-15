from fastapi import FastAPI, status
from pydantic import BaseModel



class ConnectionModel(BaseModel):
    node_url: str
    node_type: str



class Connector(FastAPI):
    def __init__(self, node, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = None
        self.port = None

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            return ConnectionModel(node_url=f"http://{self.host}:{self.port}/{self.node.name}", node_type=type(self.node).__name__)
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: ConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def set_host(self, host: str):
        self.host = host
    
    def set_port(self, port: int):
        self.port = port

    def get_connector_prefix(self):
        return f"/{self.node.name}"