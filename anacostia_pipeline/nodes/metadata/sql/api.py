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

        if server_url is not None:
            self.start_client()  # Start the client to connect to the metadata store server
            self.add_node(node_name=client_name, node_type=type(self).__name__, base_type="BaseMetadataStoreClient")

    def add_node(self, node_name: str, node_type: str, base_type: str):
        """
        Register node with metadata store on root pipeline.
        """

        async def _add_node(node_name: str, node_type: str, base_type: str):
            try:
                response = await self.client.post(
                    url="/add_node/", 
                    json={"node_name": node_name, "node_type": node_type, "base_type": base_type}
                )
                if response.status_code != 200:
                    raise Exception(f"Failed to add node: {response.status_code}, {response.text}")
            except Exception as e:
                self.log(f"Error adding node: {e}", level="ERROR")
                raise e
        
        task = asyncio.run_coroutine_threadsafe(_add_node(node_name, node_type, base_type), self.loop)
        return task.result()

    def get_run_id(self):
        """
        Get the run ID from the metadata store.
        This method sends a GET request to the server to retrieve the run ID.
        """

        async def _get_run_id():
            response = await self.client.get(f"/get_run_id/")
            run_id = response.json()["run_id"]
            return run_id

        task = asyncio.run_coroutine_threadsafe(_get_run_id(), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while getting run ID: {e}", level="ERROR")
            raise e

    def get_node_id(self, node_name: str):
        """
        Get the node ID from the metadata store.
        This method sends a GET request to the server to retrieve the node ID.
        """

        async def _get_node_id(node_name: str):
            response = await self.client.get(f"/get_node_id/?node_name={node_name}")
            node_id = response.json()["node_id"]
            return node_id

        task = asyncio.run_coroutine_threadsafe(_get_node_id(node_name), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while getting node ID: {e}", level="ERROR")
            raise e
    
    def create_entry(
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

        async def _create_entry(data: Dict):
            try:
                response = await self.client.post(f"/create_entry/", json=data)
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Create entry failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error creating entry: {e}", level="ERROR")
                raise e

        task = asyncio.run_coroutine_threadsafe(_create_entry(data), self.loop)
        return task.result()

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
    
    def entry_exists(self, resource_node_name: str, location: str):
        async def _entry_exists(resource_node_name: str, location: str):
            response = await self.client.get(f"/entry_exists/?resource_node_name={resource_node_name}&location={location}")
            exists = response.json()["exists"]
            return exists

        task = asyncio.run_coroutine_threadsafe(_entry_exists(resource_node_name, location), self.loop)
        return task.result()

    def get_num_entries(self, resource_node_name: str, state: str):
        """
        Get the number of entries for a specific resource node and state.
        This method sends a GET request to the server to retrieve the number of entries.
        """
        
        async def _get_num_entries(resource_node_name: str, state: str):
            try:
                response = await self.client.get(f"/get_num_entries/?resource_node_name={resource_node_name}&state={state}")
                num_entries = response.json()["num_entries"]
                return num_entries
            except Exception as e:
                self.log(f"Error getting number of entries: {e}", level="ERROR")
                raise e

        task = asyncio.run_coroutine_threadsafe(_get_num_entries(resource_node_name, state), self.loop)
        return task.result()

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

    def log_params(self, node_name: str, **kwargs):
        """
        Log parameters for a specific node.
        This method sends a POST request to the server to log parameters.
        """
        async def _log_params(node_name: str, **kwargs):
            try:
                response = await self.client.post(f"/log_params/?node_name={node_name}", json=kwargs)
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Log params failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error logging params: {e}", level="ERROR")
                raise e

        task = asyncio.run_coroutine_threadsafe(_log_params(node_name, **kwargs), self.loop)
        return task.result()

    def set_tags(self, node_name: str, **kwargs):
        """
        Set tags for a specific node.
        This method sends a POST request to the server to set tags.
        """
        async def _set_tags(node_name: str, **kwargs):
            try:
                response = await self.client.post(f"/set_tags/?node_name={node_name}", json=kwargs)
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Set tags failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error setting tags: {e}", level="ERROR")
                raise e

        task = asyncio.run_coroutine_threadsafe(_set_tags(node_name, **kwargs), self.loop)
        return task.result()

    def get_metrics(self, node_name: str = None, run_id: int = None):
        """
        Get metrics for a specific node or run ID.
        This method sends a GET request to the server to retrieve metrics.
        """

        async def _get_metrics(node_name: str = None, run_id: int = None):

            if node_name is not None and run_id is not None:
                response = await self.client.get(f"/get_metrics/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await self.client.get(f"/get_metrics/?node_name={node_name}")
            elif run_id is not None:
                response = await self.client.get(f"/get_metrics/?run_id={run_id}")
            else:
                response = await self.client.get(f"/get_metrics/")

            metrics = response.json()
            return metrics

        task = asyncio.run_coroutine_threadsafe(_get_metrics(node_name, run_id), self.loop)
        return task.result()
    
    def get_params(self, node_name: str = None, run_id: int = None):
        async def _get_params(node_name: str = None, run_id: int = None):

            if node_name is not None and run_id is not None:
                response = await self.client.get(f"/get_params/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await self.client.get(f"/get_params/?node_name={node_name}")
            elif run_id is not None:
                response = await self.client.get(f"/get_params/?run_id={run_id}")
            else:
                response = await self.client.get(f"/get_params/")

            params = response.json()
            return params
        
        task = asyncio.run_coroutine_threadsafe(_get_params(node_name, run_id), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while getting params: {e}", level="ERROR")
            raise e
    
    def get_tags(self, node_name: str = None, run_id: int = None):
        async def _get_tags(node_name: str = None, run_id: int = None):

            if node_name is not None and run_id is not None:
                response = await self.client.get(f"/get_tags/?node_name={node_name}&run_id={run_id}")
            elif node_name is not None:
                response = await self.client.get(f"/get_tags/?node_name={node_name}")
            elif run_id is not None:
                response = await self.client.get(f"/get_tags/?run_id={run_id}")
            else:
                response = await self.client.get(f"/get_tags/")

            tags = response.json()
            return tags

        task = asyncio.run_coroutine_threadsafe(_get_tags(node_name, run_id), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while getting tags: {e}", level="ERROR")
            raise e

    def log_trigger(self, node_name: str, message: str = None):
        """
        Log a trigger for a specific node.
        This method sends a POST request to the server to log the trigger.
        """

        async def _log_trigger(node_name: str, message: str = None):
            try:
                response = await self.client.post(f"/log_trigger/?node_name={node_name}", json={"message": message})
                if response.status_code != status.HTTP_200_OK:
                    raise ValueError(f"Log trigger failed with status code {response.status_code}")
            except Exception as e:
                self.log(f"Error logging trigger for node {node_name}: {e}", level="ERROR")

        task = asyncio.run_coroutine_threadsafe(_log_trigger(node_name, message), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while logging trigger: {e}", level="ERROR")
            raise e

    def get_entries(self, resource_node_name: str, state: str = "all") -> List[Dict]:
        """
        Get entries from the metadata store for a specific resource node and state.
        This method sends a GET request to the server to retrieve entries.
        """

        async def _get_entries(resource_node_name: str, state: str):
            response = await self.client.get(f"/get_entries/?resource_node_name={resource_node_name}&state={state}")
            entries = response.json()

            for entry in entries:
                entry["created_at"] = datetime.fromisoformat(entry["created_at"])

            return entries

        task = asyncio.run_coroutine_threadsafe(_get_entries(resource_node_name, state), self.loop)
        try:
            result = task.result()
            return result
        except Exception as e:
            self.log(f"Error occurred while getting entries: {e}", level="ERROR")
            raise e