import threading
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.engine.pipeline import Pipeline, LeafPipeline
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver, LeafPipelineWebserver
from anacostia_pipeline.actions.network import SenderNode, ReceiverNode



class AnacostiaService(FastAPI):

    def __init__(self, name: str, pipeline: Pipeline, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):
        
        # lifespan context manager for spinning up and down the Service
        @asynccontextmanager
        async def lifespan(app: AnacostiaService):
            print(f"Opening client for service '{app.name}'")
            self.logger.info(f"Opening client for service '{app.name}'")
            app.client = httpx.AsyncClient()
            
            yield

            print(f"Closing client for {app.name}")
            self.logger.info(f"Closing client for service '{app.name}'")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)

        self.host = host
        self.port = port
        self.logger = logger
        self.name = name
        self.pipeline = pipeline
        
        self.client: httpx.AsyncClient = None
        
    def run(self):
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



class RootServiceData(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    sender_name: str
    receiver_name: str



class LeafServiceData(BaseModel):
    name: str
    host: str
    port: int
    pipeline_id: str



class RootService(AnacostiaService):
    def __init__(self, name: str, pipeline: Pipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):
        super().__init__(name, pipeline, host=host, port=port, logger=logger, *args, **kwargs)
        self.logger.info(f"Root service '{self.host}:{self.port}' started")
        self.connections = []
        self.leaf_ip_addresses = []

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

                    node_subapp = node.get_app()
                    node_subapp.set_client(self.client)
            
            # Extract the leaf ip addresses from the connections
            for connection in self.connections:
                pipeline_ip_address = f"{connection['leaf_host']}:{connection['leaf_port']}"
                if pipeline_ip_address not in self.leaf_ip_addresses:
                    self.leaf_ip_addresses.append(f"{connection['leaf_host']}:{connection['leaf_port']}")
            
            self.logger.info(f"Root service '{self.name}' connected to leaf services at ip addresses: {self.leaf_ip_addresses}")
            
            try:
                # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
                # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
                # created in the lifespan context manager in the AnacostiaService class.
                # See this video: https://www.youtube.com/watch?v=row-SdNdHFE

                # Send a healthcheck request to each leaf service
                tasks = []
                for ip_address in self.leaf_ip_addresses:
                    tasks.append(self.client.post(f"http://{ip_address}/healthcheck"))

                responses = await asyncio.gather(*tasks)

                for response in responses:
                    response_data = response.json()
                    if response_data["status"] == "ok":
                        self.logger.info(f"Successfully connected to leaf at {ip_address}")
                
            except Exception as e:
                print(f"Failed to connect to leaf at {ip_address} with error: {e}")
                self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")

    def run(self):
        # Note: we do not need to create a pipeline ID for the root service because there is only one root pipeline
        # leaf services create pipeline IDs because leaf services can connect to and spin up multiple pipelines for multiple services 
        pipeline_server = PipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
        self.mount(f"/", pipeline_server)
        super().run()


class LeafService(AnacostiaService):
    def __init__(self, name: str, pipeline: LeafPipeline, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):
        super().__init__(name, host=host, pipeline=None, port=port, logger=logger, *args, **kwargs)
        self.logger.info(f"Leaf service '{self.host}:{self.port}' started")
        self.pipeline = pipeline

        @self.post("/healthcheck", status_code=status.HTTP_200_OK)
        async def healthcheck():
            return {"status": "ok"}

        @self.post("/create_pipeline", status_code=status.HTTP_200_OK)
        async def create_pipeline():
            pipeline_id = uuid.uuid4().hex
            pipeline_server = LeafPipelineWebserver(name="pipeline", pipeline=self.pipeline, host=self.host, port=self.port)
            self.mount(f"/{pipeline_id}", pipeline_server)
            self.logger.info(f"Leaf service '{self.name}' created pipeline '{pipeline_id}'")
            leaf_data = {
                "pipeline_id": pipeline_id
            }
            return leaf_data

        @self.post("/connect_node", status_code=status.HTTP_200_OK)
        async def connect(request: Request, root_service_data: RootServiceData):
            self.logger.info(f"Leaf receiver node '{root_service_data.receiver_name}' connected to root sender node '{root_service_data.sender_name}'")
        
    def run(self):
        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        super().run()