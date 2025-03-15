from fastapi import Request
import httpx
import asyncio

from anacostia_pipeline.nodes.api import BaseAPICaller, BaseAPICallee



class SqliteMetadataStoreAPICaller(BaseAPICaller):
    def __init__(self, metadata_store, *args, **kwargs):
        super().__init__(node=metadata_store, callee_name="SqliteMetadataStoreAPICallee", *args, **kwargs)
        self.metadata_store = metadata_store

        @self.post("/log_metrics")
        async def log_metrics(request: Request):
            data = await request.json()
            self.metadata_store.log_metrics(self.metadata_store, **data)



class SqliteMetadataStoreAPICallee(BaseAPICallee):
    def __init__(self, *args, **kwargs):
        super().__init__(callee_name="SqliteMetadataStoreAPICallee", *args, **kwargs)
    
    def log_metrics(self, **data):

        async def __log_metrics(**data):
            url = f"{self.get_base_url()}/log_metrics"
            async with httpx.AsyncClient() as client:
                await client.post(url, json=data)
        
        asyncio.run(__log_metrics(**data))
    