from fastapi import FastAPI, status
from pydantic import BaseModel



class ConnectionModel(BaseModel):
    node_url: str
    node_name: str
    node_type: str



class Connector(FastAPI):
    def __init__(self, node, host: str, port: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(root: ConnectionModel) -> ConnectionModel:
            self.node.add_remote_predecessor(root.node_url)
            return ConnectionModel(
                node_url=f"http://{self.host}:{self.port}{self.get_connector_prefix()}", 
                node_name=self.node.name, 
                node_type=type(self.node).__name__
            )
        
        @self.post("/forward_signal", status_code=status.HTTP_200_OK)
        async def forward_signal(root: ConnectionModel):
            self.node.predecessors_events[root.node_url].set()
            return {"message": "Signalled predecessors"}

        @self.post("/backward_signal", status_code=status.HTTP_200_OK)
        async def backward_signal(leaf: ConnectionModel):
            self.node.successor_events[leaf.node_url].set()
            return {"message": "Signalled predecessors"}
    
    def get_connector_prefix(self):
        # sample output: /metadata/connector
        return f"/{self.node.name}/connector"
    
    def get_connect_url(self):
        # sample output: http://localhost:8000/metadata/connector/connect
        return f"http://{self.host}:{self.port}{self.get_connector_prefix()}/connect"

    def get_forward_signal_url(self):
        # sample output: http://localhost:8000/metadata/connector/forward_signal
        return f"http://{self.host}:{self.port}{self.get_connector_prefix()}/forward_signal"
    
    def get_backward_signal_url(self):
        # sample output: http://localhost:8000/metadata/connector/backward_signal
        return f"http://{self.host}:{self.port}{self.get_connector_prefix()}/backward_signal"