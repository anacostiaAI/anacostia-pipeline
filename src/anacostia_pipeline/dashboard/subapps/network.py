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

        self.leaf_pipeline_id = None

        @self.post("/signal_root", status_code=status.HTTP_200_OK)
        async def signal_root():
            self.node.logger.info(f"Root {self.node.name} received signal")
            self.node.wait_successor_event.set()
            return {"message": "Success"}

    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    def signal_successors(self, result: Result):
        signal_url = f"http://{self.reciever_host}:{self.reciever_port}/{self.leaf_pipeline_id}/{self.receiver_node_name}/signal_leaf"
        result = self.client.post(signal_url, json=result)
        self.node.log(f"Root node {self.node.name} signaled successors {signal_url}, result = {result}", level="INFO")
    
    # Note: we need to change the way SenderNode waits for successors otherwise, the SenderNode immediately signals the predecessor nodes 
    # because it doesn't have a successor node declared in the graph.  



class ReceiverNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(node, use_default_router, *args, **kwargs)
        self.sender_host: str = None
        self.sender_port: int = None

        # http://localhost:8002/shakespearl_leaf_receiver/signal_leaf

        @self.post("/signal_leaf", status_code=status.HTTP_200_OK)
        async def signal_successor():
            self.node.log(f"Leaf {self.node.name} signaled", level="INFO")
            self.node.wait_predecessor_event.set()
            return {"message": "Success"}
    
    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    def set_sender(self, sender_host: str, sender_port: int):
        self.sender_host = sender_host
        self.sender_port = sender_port
    
    def signal_predecessors(self, result: Result):
        signal_url = f"http://{self.sender_host}:{self.sender_port}/signal_root"
        self.node.log(f"Leaf node {self.node.name} signaled predecessors", level="INFO")
        self.client.post(signal_url, json=result)
        self.node.log(f"Root node {self.node.name} signaled successors {signal_url}, result = {result}", level="INFO")
