import threading
import signal
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

import uvicorn
import httpx

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.network.receiver.node import ReceiverNode
from anacostia_pipeline.nodes.network.sender.node import SenderNode
from anacostia_pipeline.nodes.network.utils import ConnectionModel
from anacostia_pipeline.nodes.app import BaseApp
from anacostia_pipeline.utils.basics import log



class LeafSubApp(FastAPI):
    def __init__(
        self, 
        pipeline: LeafPipeline, 
        root_data: List[ConnectionModel], 
        host="127.0.0.1", 
        port=8000, 
        logger: Logger = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.root_data = root_data
        self.host = host
        self.port = port
        self.client = httpx.AsyncClient()
        self.logger = logger
        self.pipeline_id = uuid.uuid4().hex      # figure out a way to merge this pipeline_id with the pipeline_id in LeafServiceApp

        for node_data in root_data:
            for node in self.pipeline.nodes:
                subapp = node.get_app()

                if isinstance(node, ReceiverNode) or isinstance(node, SenderNode):
                    subapp.set_leaf_pipeline_id(self.pipeline_id)
                    if node.name == node_data.receiver_name:
                        subapp.set_sender(node_data.sender_name, node_data.root_host, node_data.root_port)
                
                self.mount(subapp.get_node_prefix(), subapp)       # mount the BaseNodeApp to PipelineWebserver
    
    def get_pipeline_id(self):
        return self.pipeline_id
    
    def start_pipeline(self):
        self.pipeline.launch_nodes()
    
    def stop_pipeline(self):
        self.pipeline.terminate_nodes()

    def frontend_json(self):
        leaf_data = {"pipeline_id": self.pipeline_id}
        leaf_data["nodes"] = self.pipeline.model().model_dump()["nodes"]

        edges = []
        for leaf_data_node, leaf_node in zip(leaf_data["nodes"], self.pipeline.nodes):
            subapp: BaseApp = leaf_node.get_app()
            leaf_data_node["id"] = leaf_data_node["name"]
            leaf_data_node["label"] = leaf_data_node["name"].replace("_", " ")

            leaf_data_node["origin_url"] = f"http://{self.host}:{self.port}/{self.pipeline_id}"
            leaf_data_node["endpoint"] = f"http://{self.host}:{self.port}/{self.pipeline_id}{subapp.get_endpoint()}"
            leaf_data_node["status_endpoint"] = f"http://{self.host}:{self.port}/{self.pipeline_id}{subapp.get_status_endpoint()}"
            leaf_data_node["work_endpoint"] = f"http://{self.host}:{self.port}/{self.pipeline_id}{subapp.get_work_endpoint()}"

            edges_from_node = [
                { 
                    "source": leaf_data_node["id"], "target": successor, 
                    "event_name": f"{leaf_data_node['id']}_{successor}_change_edge_color" 
                } 
                for successor in leaf_data_node["successors"]
            ]
            edges.extend(edges_from_node)

        leaf_data["edges"] = edges
        return leaf_data
    


class LeafPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafPipelineApp):
            log(logger, f"Opening client for service '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    subapp = route.app

                    if isinstance(subapp, LeafSubApp):
                        log(logger, f"Closing client for webserver '{subapp.get_pipeline_id()}'")
                        await subapp.client.aclose()

            log(logger, f"Closing client for service '{app.name}'")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.client = httpx.AsyncClient()

        self.connections = { node.name: None for node in pipeline.nodes if isinstance(node, ReceiverNode) }

        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)

        self.pipelines: Dict[str, LeafSubApp] = {}

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/create_pipeline", status_code=status.HTTP_200_OK)
        def create_pipeline(root_service_node_data: List[ConnectionModel]):
            # Note: be careful here with passing self.pipeline to the LeafPipelineApp, we want to create a new instance of the pipeline
            pipeline_server = LeafSubApp(pipeline=self.pipeline, root_data=root_service_node_data, host=self.host, port=self.port)
            pipeline_id = pipeline_server.get_pipeline_id()
            self.mount(f"/{pipeline_id}", pipeline_server)
            log(self.logger, f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
            self.pipelines[pipeline_id] = pipeline_server

            pipeline_server.start_pipeline()
            return pipeline_server.frontend_json()
        
    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            for pipeline_id, pipeline in self.pipelines.items():
                print(f"Killing pipeline '{pipeline_id}'")
                pipeline.stop_pipeline()

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()