import threading
import os
import sys
import signal
import asyncio
import uuid
from logging import Logger
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
import uvicorn
import httpx

from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver



class AnacostiaService(FastAPI):

    def __init__(self, name: str, host: str = "localhost", port: int = 8000, logger=Logger, *args, **kwargs):
        
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
        
        @self.get("/connect")
        def connect(request: Request):
            # http://localhost:8000/connect
            client_ip = request.client.host

            pipeline_name = f"pipeline-{uuid.uuid4().hex}"
            return f"{client_ip} has permission to connect to the pipeline"
        
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
