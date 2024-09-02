from fastapi import status
import httpx

from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from anacostia_pipeline.engine.constants import Result



class SenderNodeApp(BaseNodeApp):
    def __init__(self, node, reciever_host: str, reciever_port: int, receiver_node_name: str, use_default_router=True, *args, **kwargs):
        super().__init__(node, use_default_router, *args, **kwargs)
        self.reciever_host = reciever_host
        self.reciever_port = reciever_port
        self.receiver_node_name = receiver_node_name
        self.client: httpx.AsyncClient = None

        self.leaf_pipeline_id = None

        @self.post("/signal_root", status_code=status.HTTP_200_OK)
        async def signal_root():
            self.node.logger.info(f"Root {self.node.name} received signal")
            self.node.leaf_signal_received = True

    def set_client(self, client):
        self.client = client

    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    async def signal_successors(self, result: Result):
        signal_url = f"http://{self.reciever_host}:{self.reciever_port}/{self.leaf_pipeline_id}/{self.receiver_node_name}/signal_leaf"
        response = await self.client.post(signal_url, json=result)
    
    # Note: we need to change the way SenderNode waits for successors otherwise, the SenderNode immediately signals the predecessor nodes 
    # because it doesn't have a successor node declared in the graph.  



class ReceiverNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(node, use_default_router, *args, **kwargs)
        self.sender_host: str = None
        self.sender_port: int = None
        self.client: httpx.AsyncClient = None

        @self.post("/signal_leaf", status_code=status.HTTP_200_OK)
        async def signal_successor():
            self.node.log(f"Leaf {self.node.name} signaled", level="INFO")
            self.node.event.set()
    
    def set_client(self, client):
        self.client = client

    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    def set_sender(self, sender_host: str, sender_port: int):
        self.sender_host = sender_host
        self.sender_port = sender_port
    
    def signal_predecessors(self, result: Result):
        self.client.post(f"http://{self.sender_host}:{self.sender_port}/signal_root", json=result)
        #self.node.log(f"{self.node.name} signaled predecessors", level="INFO")
