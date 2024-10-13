from fastapi import status
from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from anacostia_pipeline.utils.constants import Result



class SenderApp(BaseNodeApp):
    def __init__(self, node, reciever_host: str, reciever_port: int, receiver_node_name: str, use_default_router=True, *args, **kwargs):
        super().__init__(node, use_default_router, *args, **kwargs)
        self.reciever_host = reciever_host
        self.reciever_port = reciever_port
        self.receiver_name = receiver_node_name

        self.leaf_pipeline_id = None

        @self.post("/signal_root", status_code=status.HTTP_200_OK)
        async def signal_root():
            self.node.wait_receiver_node.set()
            return {"message": "Success"}

    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    async def signal_successors(self, result: Result):
        if self.leaf_pipeline_id is not None:
            signal_url = f"http://{self.reciever_host}:{self.reciever_port}/{self.leaf_pipeline_id}/{self.receiver_name}/signal_leaf"
        else:
            signal_url = f"http://{self.reciever_host}:{self.reciever_port}/{self.receiver_name}/signal_leaf"

        return await self.client.post(signal_url)