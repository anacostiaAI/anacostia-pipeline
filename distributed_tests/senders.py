from typing import List
from fastapi.requests import Request

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.network.sender.node import SenderNode
from anacostia_pipeline.nodes.network.sender.app import SenderApp
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode



class ShakespeareEvalSenderApp(SenderApp):
    def __init__(self, node, leaf_host: str, leaf_port: int, leaf_receiver: str, metadata_store: BaseMetadataStoreNode) -> None:
        super().__init__(node, leaf_host, leaf_port, leaf_receiver)
        self.metadata_store = metadata_store

        @self.post("/log_metrics")
        async def log_metrics(request: Request):
            data = await request.json()
            self.metadata_store.log_metrics(self.node, **data)



class ShakespeareEvalSender(SenderNode):
    def __init__(self, 
        name: str, leaf_host: str, leaf_port: int, leaf_receiver: str, 
        metadata_store: BaseMetadataStoreNode, predecessors: List[BaseNode]
    ) -> None:
        super().__init__(name, leaf_host, leaf_port, leaf_receiver, predecessors)
        self.metadata_store = metadata_store

        # for some reason, app must be initialized in the constructor otherwise the app will not be accessible when self.get_app() is called
        self.app = ShakespeareEvalSenderApp(self, self.leaf_host, self.leaf_port, self.leaf_receiver, self.metadata_store)

    def get_app(self):
        return self.app