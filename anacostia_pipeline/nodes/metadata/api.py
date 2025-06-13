from typing import List, Union
from logging import Logger

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.api import BaseClient, BaseServer



class BaseMetadataStoreServer(BaseServer):
    def __init__(
        self, 
        metadata_store: BaseMetadataStoreNode, 
        client_url: str, 
        host = "127.0.0.1", 
        port: int = 8000, 
        loggers: Union[Logger, List[Logger]] = None, 
        *args, 
        **kwargs
    ) -> None:
        super().__init__(metadata_store, client_url, host, port, loggers, *args, **kwargs)
        self.metadata_store = metadata_store



class BaseMetadataStoreClient(BaseClient):
    def __init__(self, client_name: str, client_host: str = "127.0.0.1", client_port: int = 8000, server_url = None, loggers = None, *args, **kwargs):
        super().__init__(client_name=client_name, client_host=client_host, client_port=client_port, server_url=server_url, loggers=loggers, *args, **kwargs)
    
    async def add_node(self, node_name: str, node_type: str, base_type: str):
        raise NotImplementedError("add_node method not implemented in SqliteMetadataRPCclient")
    
    async def get_node_id(self, node_name: str):
        raise NotImplementedError("get_node_id method not implemented in SqliteMetadataRPCclient")
    
    async def create_entry(self, resource_node_name: str, filepath: str, state: str = "new", run_id: int = None):
        raise NotImplementedError("create_entry method not implemented in SqliteMetadataRPCclient")
    
    async def merge_artifacts_table(self, resource_node_name: str, entries: List[dict]):
        raise NotImplementedError("merge_artifacts_table method not implemented in SqliteMetadataRPCclient")
    
    async def entry_exists(self, resource_node_name: str, location: str):
        raise NotImplementedError("entry_exists method not implemented in SqliteMetadataRPCclient")
    
    async def log_metrics(self, node_name: str, **kwargs):
        raise NotImplementedError("log_metrics method not implemented in SqliteMetadataRPCclient")
    
    async def log_params(self, node_name: str, **kwargs):
        raise NotImplementedError("log_params method not implemented in SqliteMetadataRPCclient")
    
    async def set_tags(self, node_name: str, **kwargs):
        raise NotImplementedError("set_tags method not implemented in SqliteMetadataRPCclient")
    
    async def get_metrics(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_metrics method not implemented in SqliteMetadataRPCclient")
    
    async def get_params(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_params method not implemented in SqliteMetadataRPCclient")
    
    async def get_tags(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_tags method not implemented in SqliteMetadataRPCclient")
        
    async def get_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_entries method not implemented in SqliteMetadataRPCclient")
    
    async def get_num_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_num_entries method not implemented in SqliteMetadataRPCclient")
    
    async def log_trigger(self, node_name: str, message: str):
        raise NotImplementedError("log_trigger method not implemented in SqliteMetadataRPCclient")
    
    async def get_num_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_num_entries method not implemented in SqliteMetadataRPCclient")