import threading
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.pipelines.leaf.app import RootPipelineApp, LeafPipelineWebserver
from anacostia_pipeline.dashboard.subapps.network import ReceiverNodeApp
from anacostia_pipeline.engine.network import SenderNode, ReceiverNode



class RootServiceData(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    root_host: str
    root_port: int
    sender_name: str
    receiver_name: str
    pipeline_id: str



class RootService(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootService):
            app.log(f"Opening client for service '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, RootPipelineApp):
                        app.log(f"Closing client for webserver '{route.app.name}'")
                        await route.app.client.aclose()

            app.log(f"Closing client for service '{app.name}'")
            # Note: we need to close the client after the lifespan context manager is done but for some reason await app.client.aclose() is throwing an error 
            # RuntimeError: unable to perform operation on <TCPTransport closed=True reading=False 0x121fa0fd0>; the handler is close
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.connections = []
        self.leaf_ip_addresses = []
        self.client = httpx.AsyncClient()

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = threading.Thread(target=self.server.run, name=name)
    
    def log(self, message: str, level: str = "INFO"):
        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(f"{level}: {message}")
    
    async def connect(self):
        # Extract data about leaf pipelines from the sender nodes in the pipeline
        for node in self.pipeline.nodes:
            if isinstance(node, SenderNode):
                connection_dict = {
                    "root_name": self.name,
                    "leaf_host": node.leaf_host,
                    "leaf_port": node.leaf_port,
                    "root_host": self.host,
                    "root_port": self.port,
                    "sender_name": node.name,
                    "receiver_name": node.leaf_receiver,
                    "pipeline_id": ""
                }
                
                if any([connection_dict["receiver_name"] == connection["receiver_name"] for connection in self.connections]):
                    raise ValueError(f"Duplicate receiver name '{connection_dict['receiver_name']}' found in the pipeline")
                else:
                    self.connections.append(connection_dict)

            # Extract the leaf ip addresses from the connections
            for connection in self.connections:
                pipeline_ip_address = f"{connection['leaf_host']}:{connection['leaf_port']}"
                if pipeline_ip_address not in self.leaf_ip_addresses:
                    self.leaf_ip_addresses.append(f"{connection['leaf_host']}:{connection['leaf_port']}")
                    self.log(f"Root service '{self.name}' beginning connection protocol to leaf services at ip addresses: {self.leaf_ip_addresses}")

        try:
            # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
            # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
            # created in the lifespan context manager in the AnacostiaService class.
            # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

            # Send a /healthcheck request to each leaf service
            self.log("------------- Healthcheck started -------------")
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                if response_data["status"] == "ok":
                    self.log(f"Successfully connected to leaf at {ip_address}")
            self.log("------------- Healthcheck completed -------------")
                
            self.log("------------- Leaf pipeline creation started -------------")
            # Send a /create_pipeline request to each leaf service and store the pipeline ID
            tasks = []
            for ip_address in self.leaf_ip_addresses:
                tasks.append(self.client.post(f"http://{ip_address}/create_pipeline", json=self.connections))
            
            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                pipeline_id = response_data["pipeline_id"]

                for node in self.pipeline.nodes:
                    if isinstance(node, SenderNode):
                        if f"{node.leaf_host}:{node.leaf_port}" == ip_address:
                            node.get_app().set_leaf_pipeline_id(pipeline_id)

                            for connection in self.connections:
                                if connection["sender_name"] == node.name:
                                    connection["pipeline_id"] = pipeline_id
            self.log("------------- Leaf pipeline creation completed -------------")

        except Exception as e:
            print(f"Failed to connect to leaf at {ip_address} with error: {e}")
            self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
            
    def run(self):
        # Note: we do not need to create a pipeline ID for the root service because there is only one root pipeline
        # leaf services create pipeline IDs because leaf services can connect to and spin up multiple pipelines for multiple services 
        pipeline_server = RootPipelineApp(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
        pipeline_server.client = httpx.AsyncClient()

        self.mount(f"/", pipeline_server)

        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # Launch the root pipeline
        self.pipeline.launch_nodes()

        # Connect to the leaf services
        asyncio.run(self.connect())


class LeafService(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafService):
            app.log(f"Opening client for service '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    subapp = route.app

                    if isinstance(subapp, LeafPipelineWebserver):
                        app.log(f"Closing client for webserver '{subapp.name}'")
                        await subapp.client.aclose()

            app.log(f"Closing client for service '{app.name}'")
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

        self.pipelines: Dict[str, LeafPipeline] = {}

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/create_pipeline", status_code=status.HTTP_200_OK)
        def create_pipeline(root_service_node_data: List[RootServiceData]):
            pipeline_id = uuid.uuid4().hex
            pipeline_server = LeafPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
            self.mount(f"/{pipeline_id}", pipeline_server)
            self.log(f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
            self.pipelines[pipeline_id] = pipeline_server.pipeline

            for node_data in root_service_node_data:
                for node in pipeline_server.pipeline.nodes:
                    if node.name == node_data.receiver_name:
                        subapp: ReceiverNodeApp = node.get_app()
                        subapp.set_sender(node_data.sender_name, node_data.root_host, node_data.root_port)
                        subapp.set_leaf_pipeline_id(pipeline_id)

            pipeline_server.pipeline.launch_nodes() 

            leaf_data = {"pipeline_id": pipeline_id}
            return leaf_data

    def log(self, message: str, level: str = "INFO"):
        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(f"{level}: {message}")

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            for pipeline_id, pipeline in self.pipelines.items():
                print(f"Killing pipeline '{pipeline_id}'")
                pipeline.terminate_nodes()

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()
