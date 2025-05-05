from typing import List, Union
from logging import Logger

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.rpc import BaseRPCCaller, BaseRPCCallee



class BaseMetadataRPCCallee(BaseRPCCallee):
    def __init__(
        self, 
        metadata_store: BaseMetadataStoreNode, 
        caller_url: str, 
        host = "127.0.0.1", 
        port: int = 8000, 
        loggers: Union[Logger, List[Logger]] = None, 
        *args, 
        **kwargs
    ) -> None:
        super().__init__(metadata_store, caller_url, host, port, loggers, *args, **kwargs)
        self.metadata_store = metadata_store



class BaseMetadataRPCCaller(BaseRPCCaller):
    def __init__(self, caller_name: str, caller_host: str = "127.0.0.1", caller_port: int = 8000, loggers = None, *args, **kwargs):
        super().__init__(caller_name, caller_host, caller_port, loggers, *args, **kwargs)
    
    async def get_node_id(self, node_name: str):
        raise NotImplementedError("get_node_id method not implemented in SqliteMetadataRPCCaller")
    
    async def create_entry(self, resource_node_name: str, filepath: str, state: str = "new", run_id: int = None):
        raise NotImplementedError("create_entry method not implemented in SqliteMetadataRPCCaller")
    
    async def merge_artifacts_table(self, resource_node_name: str, entries: List[dict]):
        raise NotImplementedError("merge_artifacts_table method not implemented in SqliteMetadataRPCCaller")
    
    async def entry_exists(self, resource_node_name: str, location: str):
        raise NotImplementedError("entry_exists method not implemented in SqliteMetadataRPCCaller")
    
    async def log_metrics(self, node_name: str, **kwargs):
        raise NotImplementedError("log_metrics method not implemented in SqliteMetadataRPCCaller")
    
    async def log_params(self, node_name: str, **kwargs):
        raise NotImplementedError("log_params method not implemented in SqliteMetadataRPCCaller")
    
    async def set_tags(self, node_name: str, **kwargs):
        raise NotImplementedError("set_tags method not implemented in SqliteMetadataRPCCaller")
    
    async def get_metrics(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_metrics method not implemented in SqliteMetadataRPCCaller")
    
    async def get_params(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_params method not implemented in SqliteMetadataRPCCaller")
    
    async def get_tags(self, node_name: str = None, run_id: int = None):
        raise NotImplementedError("get_tags method not implemented in SqliteMetadataRPCCaller")
        
    async def get_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_entries method not implemented in SqliteMetadataRPCCaller")
    
    async def get_num_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_num_entries method not implemented in SqliteMetadataRPCCaller")
    
    async def log_trigger(self, node_name: str, message: str):
        raise NotImplementedError("log_trigger method not implemented in SqliteMetadataRPCCaller")
    
    async def get_num_entries(self, resource_node_name: str, state: str):
        raise NotImplementedError("get_num_entries method not implemented in SqliteMetadataRPCCaller")