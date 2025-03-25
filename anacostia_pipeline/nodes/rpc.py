from logging import Logger
import httpx
from fastapi import FastAPI, status
from pydantic import BaseModel



class RPCConnectionModel(BaseModel):
    url: str



# provides endpoints for caller to call to execute remote procedure calls
# endpoints call methods on the node
class BaseRPCCallee(FastAPI):
    def __init__(self, node, caller_url: str, host: str = "127.0.0.1", port: int = 8000, logger: Logger = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.caller_url = caller_url
        self.logger = logger
        
    def get_node_prefix(self):
        return f"/{self.node.name}/rpc/callee"
    
    def get_callee_url(self):
        # sample output: http://127.0.0.1:8000/metadata/rpc/callee
        return f"http://{self.host}:{self.port}{self.get_node_prefix()}"
    
    async def connect(self):
        if self.caller_url is not None:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.caller_url}/rpc/caller/connect", json={"url": self.get_callee_url()})



# sends a connection request to the server
# provides methods for pipeline to call to do a remote procedure call on the node attached to the callee
class BaseRPCCaller(FastAPI):
    def __init__(self, caller_name: str, caller_host: str = "127.0.0.1", caller_port: int = 8000, logger: Logger = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.caller_host = caller_host
        self.caller_port = caller_port
        self.caller_name = caller_name
        self.callee_url = None
        self.logger = logger

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(callee: RPCConnectionModel):
            self.logger.info(f"Caller {self.caller_name} connected to callee at {callee.url}")
            self.callee_url = callee.url
    
    def get_caller_prefix(self):
        return f"/{self.caller_name}/rpc/caller"
    
    def get_caller_url(self):
        # sample output: http://127.0.0.1:8000/metadata/rpc/caller
        return f"http://{self.caller_host}:{self.caller_port}{self.get_caller_prefix()}"