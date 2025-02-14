from typing import List
from logging import Logger
import asyncio
import httpx

from anacostia_pipeline.nodes.network.receiver.node import ReceiverNode
from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp



class MetadataStoreReceiverApp(ReceiverApp):
    def __init__(self, node) -> None:
        super().__init__(node)

    async def log_metrics(self, **data):
        url = f"http://{self.sender_host}:{self.sender_port}/{self.sender_name}/log_metrics"
        async with httpx.AsyncClient() as client:
            await client.post(url, json=data)



class MetadataStoreReceiverNode(ReceiverNode):
    def __init__(self, name, loggers: List[Logger] = None) -> None:
        super().__init__(name, loggers=loggers)
        self.app: MetadataStoreReceiverApp = MetadataStoreReceiverApp(self)
    
    def get_app(self):
        return self.app
    
    def log_metrics(self, **data):
        return asyncio.run(self.app.log_metrics(**data))



class FilesystemReceiverApp(ReceiverApp):
    def __init__(self, node) -> None:
        super().__init__(node)

    async def get_file(self, file_path: str):
        url = f"http://{self.sender_host}:{self.sender_port}/{self.sender_name}/get_file"
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json={"file_path": file_path})
            return response.content



class FilesystemReceiverNode(ReceiverNode):
    def __init__(self, name, loggers: List[Logger] = None) -> None:
        super().__init__(name, loggers=loggers)
        self.app: FilesystemReceiverApp = FilesystemReceiverApp(self)
    
    def get_app(self):
        return self.app
    
    def get_file(self, file_path: str):
        return asyncio.run(self.app.get_file(file_path))