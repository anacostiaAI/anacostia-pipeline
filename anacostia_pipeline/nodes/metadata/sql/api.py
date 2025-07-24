from typing import List, Union, Dict
from logging import Logger
import json
from datetime import datetime
import asyncio

from fastapi import Request, status
import httpx

from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreServer, BaseMetadataStoreClient



class SQLMetadataStoreServer(BaseMetadataStoreServer):
    def __init__(
        self, 
        metadata_store, 
        client_url, 
        host: str = "127.0.0.1", 
        port: int = 8000, 
        loggers: Union[Logger, List[Logger]] = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            metadata_store=metadata_store,
            client_url=client_url, 
            host=host, 
            port=port, 
            loggers=loggers, 
            ssl_keyfile=ssl_keyfile, 
            ssl_certfile=ssl_certfile, 
            ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )
        self.metadata_store = metadata_store

        @self.post("/add_node/")
        async def add_node(request: Request):
            data = await request.json()
            self.metadata_store.add_node(node_name=data["node_name"], node_type=data["node_type"], base_type=data["base_type"])

        @self.get("/get_run_id/")
        async def get_run_id():
            run_id = self.metadata_store.get_run_id()
            return {"run_id": run_id}

        @self.get("/get_node_id/")
        async def get_node_id(node_name: str):
            node_id = self.metadata_store.get_node_id(node_name)
            return {"node_id": node_id}

        @self.post("/create_entry/")
        async def create_entry(request: Request):
            data = await request.json()
            
            self.metadata_store.create_entry(
                resource_node_name = data["resource_node_name"], 
                filepath = data["filepath"], 
                hash = data["hash"],
                hash_algorithm = data["hash_algorithm"],
                state = data["state"], 
                run_id = data["run_id"],
                file_size = data["file_size"],
                content_type = data["content_type"]
            )
        
        @self.post("/merge_artifacts_table/")
        async def merge_artifacts_table(resource_node_name: str, request: Request):
            entries = await request.json()
            entries = json.loads(entries)
            
            for entry in entries:
                entry["created_at"] = datetime.fromisoformat(entry["created_at"])
            
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


class SQLMetadataStoreClient(BaseMetadataStoreClient):
    def __init__(
        self, 
        client_name, 
        client_host = "127.0.0.1", 
        client_port = 8000, 
        server_url = None, 
        loggers = None, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(
            client_name=client_name, 
            client_host=client_host, 
            client_port=client_port, 
            server_url=server_url, 
            loggers=loggers, 
            ssl_keyfile=ssl_keyfile, 
            ssl_certfile=ssl_certfile, 
            ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )

    async def add_node(self, node_name: str, node_type: str, base_type: str):
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.get_server_url()}/add_node/", 
                    json={"node_name": node_name, "node_type": node_type, "base_type": base_type}
                )
                if response.status_code != 200:
                    raise Exception(f"Failed to add node: {response.status_code}, {response.text}")
            except Exception as e:
                self.log(f"Error adding node: {e}", level="ERROR")
                raise e

    async def get_run_id(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/get_run_id/")
            run_id = response.json()["run_id"]
            return run_id

    async def get_node_id(self, node_name: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/get_node_id/?node_name={node_name}")
            node_id = response.json()["node_id"]
            return node_id
    
    async def create_entry(
        self, resource_node_name: str, filepath: str, hash: str, hash_algorithm: str, 
        state: str = "new", run_id: int = None, file_size: int = None, content_type: str = None
    ):
        data = {
            "resource_node_name": resource_node_name,
            "filepath": filepath,
            "state": state,
            "run_id": run_id,
            "hash": hash,
            "hash_algorithm": hash_algorithm,
            "file_size": file_size,
            "content_type": content_type
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_server_url()}/create_entry/", json=data)
    
    def merge_artifacts_table(self, resource_node_name: str, entries: List[Dict]):
        """
        Merge artifacts table with the provided entries.
        This method sends a POST request to the server to merge the artifacts table.
        """

        async def _merge_artifacts_table(resource_node_name: str, entries: List[Dict]):
            for entry in entries:
                entry["created_at"] = entry["created_at"].isoformat()
            json_data = json.dumps(entries, indent=4)
        
            try:
                response = await self.client.post(f"/merge_artifacts_table/?resource_node_name={resource_node_name}", json=json_data)
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Merge artifacts table failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error merging artifacts table: {e}", level="ERROR")
                raise e
        
        task = asyncio.run_coroutine_threadsafe(_merge_artifacts_table(resource_node_name, entries), self.loop)
        return task.result()
    
    async def entry_exists(self, resource_node_name: str, location: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/entry_exists/?resource_node_name={resource_node_name}&location={location}")
            exists = response.json()["exists"]
            return exists

    async def get_num_entries(self, resource_node_name: str, state: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/get_num_entries/?resource_node_name={resource_node_name}&state={state}")
            num_entries = response.json()["num_entries"]
            return num_entries

    def log_metrics(self, node_name: str, **kwargs):
        """
        Log metrics for a specific node.
        This method sends a POST request to the server to log metrics.
        """

        async def _log_metrics(node_name: str, **kwargs):
            try:
                response = await self.client.post(f"/log_metrics/?node_name={node_name}", json=kwargs)
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Log metrics failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error logging metrics: {e}", level="ERROR")
                raise e

        task = asyncio.run_coroutine_threadsafe(_log_metrics(node_name, **kwargs), self.loop)
        return task.result()

    async def log_params(self, node_name: str, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_server_url()}/log_params/?node_name={node_name}", json=kwargs)
    
    async def set_tags(self, node_name: str, **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_server_url()}/set_tags/?node_name={node_name}", json=kwargs)
    
    async def get_metrics(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_metrics/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_server_url()}/get_metrics/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_metrics/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_server_url()}/get_metrics/")

            metrics = response.json()
            return metrics
    
    async def get_params(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_params/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_server_url()}/get_params/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_params/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_server_url()}/get_params/")

            params = response.json()
            return params
    
    async def get_tags(self, node_name: str = None, run_id: int = None):
        async with httpx.AsyncClient() as client:

            if node_name is not None and run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_tags/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await client.get(f"{self.get_server_url()}/get_tags/?node_name={node_name}")
            elif run_id is not None:
                response = await client.get(f"{self.get_server_url()}/get_tags/?run_id={run_id}")
            else:
                response = await client.get(f"{self.get_server_url()}/get_tags/")

            tags = response.json()
            return tags
    
    async def log_trigger(self, node_name: str, message: str = None):
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.get_server_url()}/log_trigger/?node_name={node_name}", json={"message": message})

    async def get_num_entries(self, resource_node_name: str, state: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/get_num_entries/?resource_node_name={resource_node_name}&state={state}")
            num_entries = response.json()["num_entries"]
            return num_entries
    
    async def get_entries(self, resource_node_name: str, state: str = "all") -> List[Dict]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.get_server_url()}/get_entries/?resource_node_name={resource_node_name}&state={state}")
            entries = response.json()

            for entry in entries:
                entry["created_at"] = datetime.fromisoformat(entry["created_at"])

            return entries