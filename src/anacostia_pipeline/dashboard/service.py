from threading import Thread
import os
import sys
import signal
import asyncio
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn
import httpx

from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver



class AnacostiaService(FastAPI):

    def __init__(self, host: str = "localhost", port: int = 8000, *args, **kwargs):
        
        # lifespan context manager for spinning up and down the Service
        @asynccontextmanager
        async def life(app: AnacostiaService):
            print("opening client")
            app.client = httpx.AsyncClient()
            yield
            print("closing client")
            await app.client.aclose()
        
        super().__init__(lifespan=life, *args, **kwargs)
        self.host = host
        self.port = port
        
        self.client: httpx.AsyncClient = None
        
        @self.get("/")
        def service_info():
            # http://localhost:8000/
            return f"in the future, display the graph of the pipeline here"
        
        @self.get("/connect")
        def connect():
            # http://localhost:8000/connect
            pipeline_name = f"pipeline-{uuid.uuid4().hex}"
            return f"connect to the pipeline"
        
    def run(self):
        config = uvicorn.Config(self, host=self.host, port=self.port)
        server = uvicorn.Server(config)
        server.run()
