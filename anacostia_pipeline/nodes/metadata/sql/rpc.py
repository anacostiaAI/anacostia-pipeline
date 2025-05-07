from typing import List, Union, Dict
from logging import Logger
import json
from datetime import datetime

from fastapi import Request
import httpx

from anacostia_pipeline.nodes.metadata.rpc import BaseMetadataRPCCallee, BaseMetadataRPCCaller



class SQLMetadataRPCCallee(BaseMetadataRPCCallee):
    def __init__(self, metadata_store, caller_url, host = "127.0.0.1", port: int = 8000, loggers: Union[Logger, List[Logger]] = None, *args, **kwargs):
        super().__init__(metadata_store, caller_url, host, port, loggers, *args, **kwargs)
        self.metadata_store = metadata_store

        @self.get("/get_run_id/")
        async def get_run_id():
            run_id = self.metadata_store.get_run_id()
            return {"run_id": run_id}

        @self.get("/get_node_id/")
        async def get_node_id(node_name: str):
            node_id = self.metadata_store.get_node_id(node_name)
            return {"node_id": node_id}

        @self.get("/create_entry/")
        async def create_entry(resource_node_name: str, filepath: str, state: str = "new", run_id: int = None):
            self.metadata_store.create_entry(resource_node_name, filepath, state, run_id)
        
        @self.post("/merge_artifacts_table/")
        async def merge_artifacts_table(resource_node_name: str, request: Request):
            entries = await request.json()
            entries = json.loads(entries)
            
            for entry in entries:
                entry["created_at"] = datetime.fromisoformat(entry["created_at"])
                if entry["end_time"] is not None:
                    entry["end_time"] = datetime.fromisoformat(entry["end_time"])
            
            self.metadata_store.merge_artifacts_table(resource_node_name, entries)
        
        @self.get("/entry_exists/")
        async def entry_exists(resource_node_name: str, location: str):
            exists = self.metadata_store.entry_exists(resource_node_name, location)
            if exists:
                return {"exists": True}
            else:
                return {"exists": False}

        @self.post("/log_metrics/")
        async def log_metrics(node_name: str, request: Request):
            data = await request.json()
            self.metadata_store.log_metrics(node_name, **data)
        
        @self.post("/log_params/")
        async def log_params(node_name: str, request: Request):
            data = await request.json()
            self.metadata_store.log_params(node_name, **data)

        @self.post("/set_tags/")
        async def set_tags(node_name: str, request: Request):
            data = await request.json()
            self.metadata_store.set_tags(node_name, **data)
        
        @self.get("/get_metrics/")
        async def get_metrics(node_name: str = None, run_id: int = None):
            metrics = self.metadata_store.get_metrics(node_name=node_name, run_id=run_id)
            return metrics
        
        @self.get("/get_params/")
        async def get_params(node_name: str = None, run_id: int = None):
            params = self.metadata_store.get_params(node_name=node_name, run_id=run_id)
            return params

        @self.get("/get_tags/")
        async def get_tags(node_name: str = None, run_id: int = None):
            tags = self.metadata_store.get_tags(node_name=node_name, run_id=run_id)
            return tags
        
        @self.post("/log_trigger/")
        async def log_trigger(node_name: str, request: Request):
            data = await request.json()
            self.metadata_store.log_trigger(node_name, message=data["message"])
        
        @self.get("/get_num_entries/")
        async def get_num_entries(resource_node_name: str, state: str):
            num_entries = self.metadata_store.get_num_entries(resource_node_name, state)
            return {"num_entries": num_entries}
        
        @self.get("/get_entries/")
        async def get_entries(resource_node_name: str, state: str):
            entries = self.metadata_store.get_entries(resource_node_name, state)
            return entries


class SQLMetadataRPCCaller(BaseMetadataRPCCaller):
    def __init__(self, caller_name, caller_host = "127.0.0.1", caller_port = 8000, loggers = None, *args, **kwargs):
        super().__init__(caller_name, caller_host, caller_port, loggers, *args, **kwargs)
    
    async def get_run_id(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_run_id/")
            run_id = response.json()["run_id"]
            return run_id

    async def get_node_id(self, node_name: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_node_id/?node_name={node_name}")
            node_id = response.json()["node_id"]
            return node_id
    
    async def create_entry(self, resource_node_name: str, filepath: str, state: str = "new", run_id: int = None):
        async with httpx.AsyncClient() as client:
            if run_id is not None:
                response = await client.get(
                    f"{self.get_callee_url()}/create_entry/?resource_node_name={resource_node_name}&filepath={filepath}&state={state}&run_id={run_id}"
                )
            else:
                response = await client.get(
                    f"{self.get_callee_url()}/create_entry/?resource_node_name={resource_node_name}&filepath={filepath}&state={state}"
                )
    
    async def merge_artifacts_table(self, resource_node_name: str, entries: List[Dict]):
        async with httpx.AsyncClient() as client:
            for entry in entries:
                entry["created_at"] = entry["created_at"].isoformat()
            json_data = json.dumps(entries, indent=4)
            response = await client.post(f"{self.get_callee_url()}/merge_artifacts_table/?resource_node_name={resource_node_name}", json=json_data)
    
    async def entry_exists(self, resource_node_name: str, location: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/entry_exists/?resource_node_name={resource_node_name}&location={location}")
            exists = response.json()["exists"]
            return exists

    async def get_num_entries(self, resource_node_name: str, state: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_num_entries/?resource_node_name={resource_node_name}&state={state}")
            num_entries = response.json()["num_entries"]
            return num_entries

    async def log_metrics(self, node_name: str, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/log_metrics/?node_name={node_name}", json=kwargs)
    
    async def log_params(self, node_name: str, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/log_params/?node_name={node_name}", json=kwargs)
    
    async def set_tags(self, node_name: str, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/set_tags/?node_name={node_name}", json=kwargs)
    
    async def get_metrics(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_metrics/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_callee_url()}/get_metrics/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_metrics/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_callee_url()}/get_metrics/")

            metrics = response.json()
            return metrics
    
    async def get_params(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_params/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_callee_url()}/get_params/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_params/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_callee_url()}/get_params/")

            params = response.json()
            return params
    
    async def get_tags(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_tags/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_callee_url()}/get_tags/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_callee_url()}/get_tags/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_callee_url()}/get_tags/")

            tags = response.json()
            return tags
    
    async def log_trigger(self, node_name: str, message: str = None):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_callee_url()}/log_trigger/?node_name={node_name}", json={"message": message})

    async def get_num_entries(self, resource_node_name: str, state: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_num_entries/?resource_node_name={resource_node_name}&state={state}")
            num_entries = response.json()["num_entries"]
            return num_entries
    
    async def get_entries(self, resource_node_name: str, state: str = "all") -> List[Dict]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_callee_url()}/get_entries/?resource_node_name={resource_node_name}&state={state}")
            entries = response.json()

            for entry in entries:
                entry["created_at"] = datetime.fromisoformat(entry["created_at"])

                if entry["end_time"] is not None:
                    entry["end_time"] = datetime.fromisoformat(entry["end_time"])
            
            return entries