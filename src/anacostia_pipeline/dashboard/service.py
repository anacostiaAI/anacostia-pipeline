import threading
import os
import sys
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
import uvicorn
import httpx
from pydantic import BaseModel

from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver
from anacostia_pipeline.actions.network import SenderNode, ReceiverNode
from anacostia_pipeline.engine.constants import Result



class AnacostiaService(FastAPI):

    def __init__(self, name: str, host: str = "localhost", port: int = 8000, logger: Logger = None, *args, **kwargs):
        
        # lifespan context manager for spinning up and down the Service
        @asynccontextmanager
        async def lifespan(app: AnacostiaService):
            print("opening client")
            app.client = httpx.AsyncClient()
            yield
            print("closing client")
            await app.client.aclose()
        
        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.host = host
        self.port = port

        self.logger = logger
        self.name = name
        
        self.client: httpx.AsyncClient = None
        
        @self.get("/")
        def service_info():
            # http://localhost:8000/
            return f"Hello from {name}, in the future, display the graph of the pipeline here"
        
    
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

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        fastapi_thread.start()



class RootServiceData(BaseModel):
    name: str
    host: str
    port: int
    pipeline_id: str



class LeafServiceData(BaseModel):
    name: str
    host: str
    port: int
    pipeline_id: str



class RootService(AnacostiaService):
    def __init__(self, name: str, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):
        super().__init__(name, host=host, port=port, logger=logger, *args, **kwargs)
        self.logger.info(f"Root service '{self.host}:{self.port}' started")

        @self.get("/connect", status_code=status.HTTP_200_OK)
        async def connect(request: Request):
            pipeline_id = f"pipeline-{uuid.uuid4().hex}"

            leaf_ip_adresses = ["192.168.100.2:8002"]

            for ip_address in leaf_ip_adresses:
                try:
                    root_data = {
                        "name": self.name,
                        "host": self.host,
                        "port": self.port,
                        "pipeline_id": pipeline_id
                    }

                    # Note: don't use httpx.post here, it will throw an error "object Response can't be used in 'await' expression"
                    # Instead, we use await self.client.post because we already have an httpx.AsyncClient() object 
                    # created in the lifespan context manager in the AnacostiaService class.
                    # See this video: https://www.youtube.com/watch?v=row-SdNdHFE
                    response = await self.client.post(f"http://{ip_address}/connect_leaf", json=root_data)
                    response_data = response.json()
                    self.logger.info(response_data)
                    print(response_data)

                except Exception as e:
                    print(f"Failed to connect to leaf at {ip_address} with error: {e}")
                    self.logger.error(f"Failed to connect to leaf at {ip_address} with error: {e}")
        
            @self.post("signal_predecessor", status_code=status.HTTP_200_OK)
            async def signal_predecessor(request: Request, result: Result):
                self.logger.info(f"Signal received from leaf service '{self.name}'")
                return {"message": "success"}
        
    async def signal_successors(self, sender_node: SenderNode, result: Result):
        pipeline_id = f"pipeline-{uuid.uuid4().hex}"
        leaf_ip_adresses = sender_node.leaf_url
        for ip_address in leaf_ip_adresses:
            response = await self.client.post(f"http://{ip_address}/signal_successor", json=result)


class LeafService(AnacostiaService):
    def __init__(self, name: str, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):
        super().__init__(name, host=host, port=port, logger=logger, *args, **kwargs)
        self.logger.info(f"Leaf service '{self.host}:{self.port}' started")

        @self.post("/connect_leaf", status_code=status.HTTP_200_OK)
        async def connect(request: Request, root_service_data: RootServiceData):
            pipeline_id = f"pipeline-{uuid.uuid4().hex}"
            self.logger.info(f"Leaf service '{self.name}' connected to '{root_service_data.name} ({root_service_data.pipeline_id})' service running on '{root_service_data.host}:{root_service_data.port}'")
            
            leaf_data = {
                "name": self.name,
                "host": self.host,
                "port": self.port,
                "pipeline_id": pipeline_id
            }
            return leaf_data
        
        @self.post("/signal_successor", status_code=status.HTTP_200_OK)
        async def signal_successor(request: Request, result: Result):
            self.logger.info(f"Signal received from root service '{self.name}'")
            return {"message": "success"}
        
    async def signal_predecessors(self, reciever_node, result: Result):
        root_ip_address = reciever_node.root_url
        response = await self.client.post(f"http://{root_ip_address}/signal_predecessor", json=result)