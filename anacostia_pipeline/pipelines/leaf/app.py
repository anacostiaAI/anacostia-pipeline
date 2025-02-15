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



class LeafPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafPipelineApp):
            app.logger.info(f"Opening client for service '{app.name}'")

            # queue must be set for all nodes prior to starting the background task
            for node in app.pipeline.nodes:
                node.set_queue(app.queue)

            app.background_task = asyncio.create_task(app.process_queue())

            yield

            # Cancel and cleanup the background task when the app shuts down
            app.background_task.cancel()
            try:
                await app.background_task
            except asyncio.CancelledError:
                pass

            app.logger.info(f"Closing client for service '{app.name}'")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.root_host = None
        self.root_port = None
        self.pipeline = pipeline
        self.logger = logger
        self.client = httpx.AsyncClient()
        self.pipeline_id = uuid.uuid4().hex
        self.queue = Queue()
        self.background_task = None

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

        for node in self.pipeline.nodes:
            subapp = node.get_app()
            self.mount(subapp.get_node_prefix(), subapp)         # mount the BaseNodeApp to LeafPipelineApp 

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/connect", status_code=status.HTTP_200_OK)
        async def connect(connections: List[ConnectionModel]):
            try:
                for node_data in connections:
                    for node in self.pipeline.nodes:
                        subapp = node.get_app()

                        if isinstance(node, ReceiverNode) or isinstance(node, SenderNode):
                            if node.name == node_data.receiver_name:
                                subapp.set_sender(node_data.sender_name, node_data.root_host, node_data.root_port)

                self.root_host = connections[0].root_host
                self.root_port = connections[0].root_port 
                self.logger.info(f"Leaf service '{self.name}' created pipeline")
                return self.frontend_json()

            except Exception as e:
                self.logger.error(f"Leaf error: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create pipeline: {str(e)}"
                )
        
        self.queue = Queue()
    
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

    def frontend_json(self):
        leaf_data = {"pipeline_id": self.pipeline_id}
        leaf_data["nodes"] = self.pipeline.model().model_dump()["nodes"]

        edges = []
        for leaf_data_node, leaf_node in zip(leaf_data["nodes"], self.pipeline.nodes):
            subapp: BaseApp = leaf_node.get_app()
            leaf_data_node["id"] = leaf_data_node["name"]
            leaf_data_node["label"] = leaf_data_node["name"]
            leaf_data_node["origin_url"] = f"http://{self.host}:{self.port}"
            leaf_data_node["type"] = type(leaf_node).__name__
            leaf_data_node["endpoint"] = f"http://{self.host}:{self.port}{subapp.get_endpoint()}"
            leaf_data_node["status_endpoint"] = f"http://{self.host}:{self.port}{subapp.get_status_endpoint()}"

            edges_from_node = [
                {"source": leaf_data_node["id"], "target": successor} for successor in leaf_data_node["successors"]
            ]
            edges.extend(edges_from_node)

        leaf_data["edges"] = edges
        return leaf_data

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            self.pipeline.terminate_nodes()

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        self.pipeline.launch_nodes()

        if sys.version_info.major == 3 and sys.version_info.minor >= 12:
            # keep the main thread open; this is done to avoid an error in python 3.12 "RuntimeError: can't create new thread at interpreter shutdown"
            for thread in threading.enumerate():
                if thread.daemon or thread is threading.current_thread():
                    continue
                thread.join()