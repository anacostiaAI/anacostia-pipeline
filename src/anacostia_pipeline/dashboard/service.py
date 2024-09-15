import threading
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager
from typing import List, Dict

from fastapi import FastAPI, Request, status, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.engine.pipeline import Pipeline, LeafPipeline
from anacostia_pipeline.dashboard.subapps.pipeline import RootPipelineWebserver, LeafPipelineWebserver
from anacostia_pipeline.actions.network import SenderNode, ReceiverNode



class RootService(FastAPI):
    def __init__(self, name: str, pipeline: Pipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):

        # lifespan context manager for spinning up and shutting down the service
        @asynccontextmanager
        async def lifespan(app: RootService):
            app.log(f"Opening client for service '{app.name}'")
            # app.client = httpx.AsyncClient()

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, RootPipelineWebserver):
                        app.log(f"Closing client for webserver '{route.app.name}'")
                        await route.app.client.aclose()

            app.log(f"Closing client for service '{app.name}'")
            #await app.client.aclose()
        
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
        self.fastapi_thread = threading.Thread(target=self.server.run)
    
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
                    "sender_name": node.name,
                    "receiver_name": node.leaf_receiver
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
                tasks.append(self.client.post(f"http://{ip_address}/create_pipeline"))
            
            responses = await asyncio.gather(*tasks)

            for response in responses:
                response_data = response.json()
                pipeline_id = response_data["pipeline_id"]

                for node in self.pipeline.nodes:
                    if isinstance(node, SenderNode):
                        if f"{node.leaf_host}:{node.leaf_port}" == ip_address:
                            node.get_app().set_leaf_pipeline_id(pipeline_id)
            self.log("------------- Leaf pipeline creation completed -------------")

        except Exception as e:
            print(f"Failed to connect to leaf at {ip_address} with error: {e}")
            self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
            
    def run(self):
        # Note: we do not need to create a pipeline ID for the root service because there is only one root pipeline
        # leaf services create pipeline IDs because leaf services can connect to and spin up multiple pipelines for multiple services 
        pipeline_server = RootPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
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
            #app.client = httpx.AsyncClient()

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
        self.fastapi_thread = threading.Thread(target=self.server.run)

        self.pipelines: Dict[str, LeafPipeline] = {}

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/create_pipeline", status_code=status.HTTP_200_OK)
        def create_pipeline():
            pipeline_id = uuid.uuid4().hex
            pipeline_server = LeafPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
            self.mount(f"/{pipeline_id}", pipeline_server)
            self.log(f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
            leaf_data = {
                "pipeline_id": pipeline_id
            }

            pipeline_server.pipeline.launch_nodes() 
            self.pipelines[pipeline_id] = pipeline_server.pipeline
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


"""
class RootServiceData(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    sender_name: str
    receiver_name: str



class RootService(FastAPI):
    def __init__(self, name: str, pipeline: Pipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        # lifespan context manager for spinning up and down the Service
        @asynccontextmanager
        async def lifespan(app: RootService):
            print(f"Opening client for service '{app.name}'")
            app.logger.info(f"Opening client for service '{app.name}'")
            app.client = httpx.AsyncClient()

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, RootPipelineWebserver):
                        print(f"Closing client for subapp {route.path}")
                        app.logger.info(f"Closing client for subapp '{route.path}'")
                        await route.app.client.aclose()

            print(f"Closing client for {app.name}")
            app.logger.info(f"Closing client for service '{app.name}'")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.logger.info(f"Root service '{self.host}:{self.port}' started")
        self.connections = []
        self.leaf_ip_addresses = []
        self.client: httpx.AsyncClient = None

        @self.get("/connect", status_code=status.HTTP_200_OK)
        async def connect():
            # Extract data about leaf pipelines from the sender nodes in the pipeline
            for node in self.pipeline.nodes:
                if isinstance(node, SenderNode):
                    connection_dict = {
                        "root_name": self.name,
                        "leaf_host": node.leaf_host,
                        "leaf_port": node.leaf_port,
                        "sender_name": node.name,
                        "receiver_name": node.leaf_receiver
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
            
            self.logger.info(f"Root service '{self.name}' beginning connection protocol to leaf services at ip addresses: {self.leaf_ip_addresses}")
            
            try:
                # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
                # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
                # created in the lifespan context manager in the AnacostiaService class.
                # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

                # Send a /healthcheck request to each leaf service
                self.logger.info("------------- Healthcheck started -------------")
                tasks = []
                for ip_address in self.leaf_ip_addresses:
                    tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

                responses = await asyncio.gather(*tasks)

                for response in responses:
                    response_data = response.json()
                    if response_data["status"] == "ok":
                        self.logger.info(f"Successfully connected to leaf at {ip_address}")
                self.logger.info("------------- Healthcheck completed -------------")
                
                self.logger.info("------------- Leaf pipeline creation started -------------")
                # Send a /create_pipeline request to each leaf service and store the pipeline ID
                tasks = []
                for ip_address in self.leaf_ip_addresses:
                    tasks.append(self.client.post(f"http://{ip_address}/create_pipeline"))
                
                responses = await asyncio.gather(*tasks)

                for response in responses:
                    response_data = response.json()
                    pipeline_id = response_data["pipeline_id"]

                    for node in self.pipeline.nodes:
                        if isinstance(node, SenderNode):
                            if f"{node.leaf_host}:{node.leaf_port}" == ip_address:
                                node.set_leaf_pipeline_id(pipeline_id)
                                node.get_app().set_leaf_pipeline_id(pipeline_id)
                                self.logger.info(f"Successfully connected '{node.name}' to pipeline '{node.leaf_pipeline_id}'")
                self.logger.info("------------- Leaf pipeline creation completed -------------")
                
                self.logger.info("------------- Node connection started -------------")
                # Send a /connect_node request to each leaf service
                tasks = []
                for connection in self.connections:
                    self.logger.info(
                        f"Root service connecting root sender node '{connection['sender_name']}' to leaf receiver node '{connection['receiver_name']}'"
                    )
                    tasks.append(self.client.post(f"http://{connection['leaf_host']}:{connection['leaf_port']}/connect_node", json=connection))

                responses = await asyncio.gather(*tasks)

                for response in responses:
                    response_data = response.json()
                    self.logger.info(
                        f"Successfully connected root sender node '{response_data['sender_name']}' to leaf receiver node '{response_data['receiver_name']}'"
                    )
                self.logger.info("------------- Node connection completed -------------")

            except Exception as e:
                print(f"Failed to connect to leaf at {ip_address} with error: {e}")
                self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")

    def run(self):
        # Note: we do not need to create a pipeline ID for the root service because there is only one root pipeline
        # leaf services create pipeline IDs because leaf services can connect to and spin up multiple pipelines for multiple services 
        pipeline_server = RootPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)

        self.mount(f"/", pipeline_server)

        config = uvicorn.Config(self, host=self.host, port=self.port)
        server = uvicorn.Server(config)
        fastapi_thread = threading.Thread(target=server.run)
    
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            server.should_exit = True
            fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        fastapi_thread.start()

        self.pipeline.launch_nodes()



class LeafService(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafService):
            app.log(f"Opening client for service '{app.name}'")
            app.client = httpx.AsyncClient()

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    subapp = route.app

                    if isinstance(subapp, LeafPipelineWebserver):
                        app.log(f"Closing client for webserver '{app.name}'")
                        await subapp.client.aclose()

            app.log(f"Closing client for service '{app.name}'")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.host = host
        self.port = port
        self.pipeline = pipeline
        self.logger = logger
        self.client: httpx.AsyncClient = None

        self.connections = { node.name: None for node in pipeline.nodes if isinstance(node, ReceiverNode) }

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/create_pipeline", status_code=status.HTTP_200_OK)
        async def create_pipeline():
            pipeline_id = uuid.uuid4().hex
            pipeline_server = LeafPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
            self.mount(f"/{pipeline_id}", pipeline_server)
            self.log(f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
            leaf_data = {
                "pipeline_id": pipeline_id
            }

            pipeline_server.pipeline.launch_nodes() 

            return leaf_data

        # For each connection, we need to connect the root sender node to the leaf receiver node by saving the sender name in the connections dictionary
        @self.post("/connect_node", status_code=status.HTTP_200_OK)
        async def connect(request: Request, root_service_node_data: RootServiceData):
            if root_service_node_data.receiver_name not in self.connections.keys():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail=f"Receiver node '{root_service_node_data.receiver_name}' not found in leaf service '{self.name}'"
                )
            else:
                self.connections[root_service_node_data.receiver_name] = root_service_node_data.sender_name
                self.logger.info(f"Leaf receiver node '{root_service_node_data.receiver_name}' connected to root sender node '{root_service_node_data.sender_name}'")
                return {"sender_name": root_service_node_data.sender_name, "receiver_name": root_service_node_data.receiver_name}
        
        # Note: in the future, we need to add another endpoint to enable the leaf service connect to leaf services,
        # have those leaf services spin up their pipelines, and then connect to them.
        # This will allow the leaf service to act as a root service for other leaf services.
        # This will mean that we need a way to recursively connect to leaf services and spin up their pipelines.

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
        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        config = uvicorn.Config(self, host=self.host, port=self.port)
        server = uvicorn.Server(config)
        fastapi_thread = threading.Thread(target=server.run)
    
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _kill_webserver(sig, frame):
            print(f"\nCTRL+C Caught!; Killing {self.name} Webservice...")
            server.should_exit = True
            fastapi_thread.join()
            print(f"Anacostia Webservice {self.name} Killed...")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        fastapi_thread.start()

        self.pipeline.launch_nodes()
"""