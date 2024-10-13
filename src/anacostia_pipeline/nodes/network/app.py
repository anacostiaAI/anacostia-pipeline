from fastapi import status
from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from anacostia_pipeline.utils.constants import Result



class ReceiverNodeApp(BaseNodeApp):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(node, use_default_router, *args, **kwargs)
        self.sender_name: str = None
        self.sender_host: str = None
        self.sender_port: int = None

        # http://localhost:8002/shakespearl_leaf_receiver/signal_leaf

        @self.post("/signal_leaf", status_code=status.HTTP_200_OK)
        async def signal_successor():
            self.node.wait_sender_node.set()
            return {"message": "Success"}
    
    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    def set_sender(self, sender_name: str, sender_host: str, sender_port: int):
        self.sender_name = sender_name
        self.sender_host = sender_host
        self.sender_port = sender_port
    
    async def signal_predecessors(self, result: Result):
        signal_url = f"http://{self.sender_host}:{self.sender_port}/{self.sender_name}/signal_root"
        return await self.client.post(signal_url)
