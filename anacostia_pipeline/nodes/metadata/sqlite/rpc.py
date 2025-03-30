from typing import List, Union
from logging import Logger

from fastapi import Request
import httpx

from anacostia_pipeline.nodes.rpc import BaseRPCCaller, BaseRPCCallee



class SqliteMetadataRPCCallee(BaseRPCCallee):
    def __init__(self, metadata_store, caller_url, host = "127.0.0.1", port: int = 8000, loggers: Union[Logger, List[Logger]] = None, *args, **kwargs):
        super().__init__(metadata_store, caller_url, host, port, loggers, *args, **kwargs)
        self.metadata_store = metadata_store

        @self.post("/log_metrics")
        async def log_metrics(request: Request):
            data = await request.json()
            self.metadata_store.log_metrics(self.metadata_store.name, **data)
            #self.log("Metrics logged", level="INFO")
            #return {"message": "Metrics logged"}
        
        @self.post("/log_params")
        async def log_params(request: Request):
            data = await request.json()
            self.metadata_store.log_params(self.metadata_store.name, **data)

        @self.post("/set_tags")
        async def set_tags(request: Request):
            data = await request.json()
            self.metadata_store.set_tags(self.metadata_store.name, **data)
        
        @self.get("/get_tags")
        async def get_tags(node_name: str, run_id: int = None):
            tags = self.metadata_store.get_tags(node_name=node_name, run_id=run_id)
            return tags


class SqliteMetadataRPCCaller(BaseRPCCaller):
    def __init__(self, caller_name, caller_host = "127.0.0.1", caller_port = 8000, loggers = None, *args, **kwargs):
        super().__init__(caller_name, caller_host, caller_port, loggers, *args, **kwargs)
    
    async def log_metrics(self, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/log_metrics", json=kwargs)
            #message = response.json()["message"]
            #self.log(message, level="INFO")
    
    async def log_params(self, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/log_params", json=kwargs)
    
    async def set_tags(self, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/set_tags", json=kwargs)
    
    async def get_tags(self, node_name: str, run_id: int = None):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_tags", params={"node_name": node_name, "run_id": run_id})
            tags = response.json()
            return tags