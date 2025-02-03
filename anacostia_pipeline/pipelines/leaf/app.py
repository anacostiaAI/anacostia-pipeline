import threading
import signal
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict
import sys
from queue import Queue
import asyncio

from fastapi import FastAPI, status, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

import uvicorn
import httpx

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.network.receiver.node import ReceiverNode
from anacostia_pipeline.nodes.network.sender.node import SenderNode
from anacostia_pipeline.pipelines.utils import ConnectionModel
from anacostia_pipeline.nodes.app import BaseApp



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
        self.root_host = root_data[0].root_host
        self.root_port = root_data[0].root_port
        self.host = host
        self.port = port
        self.client = httpx.AsyncClient()
        self.logger = logger
        self.pipeline_id = uuid.uuid4().hex      # figure out a way to merge this pipeline_id with the pipeline_id in LeafServiceApp
        self.queue = Queue()

        for node_data in root_data:
            for node in self.pipeline.nodes:
                subapp = node.get_app()

                if isinstance(node, ReceiverNode) or isinstance(node, SenderNode):
                    subapp.set_leaf_pipeline_id(self.pipeline_id)
                    if node.name == node_data.receiver_name:
                        subapp.set_sender(node_data.sender_name, node_data.root_host, node_data.root_port)
                
                self.mount(subapp.get_node_prefix(), subapp)        # mount the BaseNodeApp to PipelineWebserver
                node.set_queue(self.queue)                          # set the queue for the node
    
        self.background_task = asyncio.create_task(self.process_queue())
    
    async def process_queue(self):
        while True:
            if self.queue.empty() is False:
                message = self.queue.get()
                
                try:
                    await self.client.post(f"http://{self.root_host}:{self.root_port}/send_event", json=message)
                
                except Exception as e:
                    print(f"Error forwarding message: {str(e)}")
                    self.queue.put(message)
                
                finally:
                    self.queue.task_done()
            
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                print("Background task cancelled; breaking out of queue processing loop")
                break

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
            leaf_data_node["label"] = leaf_data_node["name"]
            leaf_data_node["origin_url"] = f"http://{self.host}:{self.port}/{self.pipeline_id}"
            leaf_data_node["type"] = type(leaf_node).__name__
            leaf_data_node["endpoint"] = f"http://{self.host}:{self.port}/{self.pipeline_id}{subapp.get_endpoint()}"
            leaf_data_node["status_endpoint"] = f"http://{self.host}:{self.port}/{self.pipeline_id}{subapp.get_status_endpoint()}"

            edges_from_node = [
                {"source": leaf_data_node["id"], "target": successor} for successor in leaf_data_node["successors"]
            ]
            edges.extend(edges_from_node)

        leaf_data["edges"] = edges
        return leaf_data
    


class LeafPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafPipelineApp):
            app.logger.info(f"Opening client for service '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    subapp = route.app

                    if isinstance(subapp, LeafSubApp):
                        app.logger.info(f"Closing client for webserver '{subapp.get_pipeline_id()}'")
                        subapp.background_task.cancel()
                        await subapp.client.aclose()

            app.logger.info(f"Closing client for service '{app.name}'")
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
        async def create_pipeline(connections: List[ConnectionModel]):
            try:
                # Note: be careful here with passing self.pipeline to the LeafPipelineApp, we want to create a new instance of the pipeline
                pipeline_server = LeafSubApp(pipeline=self.pipeline, root_data=connections, host=self.host, port=self.port)
                pipeline_id = pipeline_server.get_pipeline_id()
                self.mount(f"/{pipeline_id}", pipeline_server)
                self.logger.info(f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
                self.pipelines[pipeline_id] = pipeline_server

                pipeline_server.start_pipeline()
                return pipeline_server.frontend_json()

            except Exception as e:
                self.logger.error(f"Leaf error: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create pipeline: {str(e)}"
                )
        
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

        if sys.version_info.major == 3 and sys.version_info.minor >= 12:
            # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
            for thread in threading.enumerate():
                if thread.daemon or thread is threading.current_thread():
                    continue
                thread.join()