import os
import sys
import signal
from importlib import metadata
from threading import Thread
import uvicorn
import asyncio
import httpx
import uuid
from contextlib import asynccontextmanager
from logging import Logger

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.routing import Mount

from anacostia_pipeline.pipelines.root.fragments import node_bar_closed, node_bar_open, node_bar_invisible, index_template
from anacostia_pipeline.dashboard.subapps.basenode import BaseNodeApp
from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp
from anacostia_pipeline.nodes.network.sender.app import SenderApp
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline, PipelineModel
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.utils.constants import Work



PACKAGE_NAME = "anacostia_pipeline"
DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline.dashboard"].__file__)



class LeafPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: LeafPipeline, host="127.0.0.1", port=8000, logger: Logger = None, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: LeafPipelineApp):
            app.log(f"Starting leaf pipeline '{app.name}'")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, BaseNodeApp):
                        app.log(f"Closing client for subapp '{route.path}'", level="INFO")
                        subapp: BaseNodeApp = route.app
                        await subapp.client.aclose()

            app.log(f"Closing client for leaf pipeline '{app.name}'")
            await app.client.aclose()

        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.server = None
        self.fastapi_thread = None
        self.client = httpx.AsyncClient()
        self.logger = logger

        pipeline_id = uuid.uuid4().hex

        config = uvicorn.Config(self, host=self.host, port=self.port)
        self.server = uvicorn.Server(config)
        self.fastapi_thread = Thread(target=self.server.run, name=self.name)

        for node in self.pipeline.nodes:
            node_subapp = node.get_app()

            if isinstance(node_subapp, ReceiverApp) or isinstance(node_subapp, SenderApp):
                node_subapp.set_leaf_pipeline_id(pipeline_id)

            self.mount(node_subapp.get_node_prefix(), node_subapp)       # mount the BaseNodeApp to PipelineWebserver
    
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

    def stop(self):
        if (self.server is not None) and (self.fastapi_thread is not None):
            self.server.should_exit = True
            self.fastapi_thread.join()
            self.pipeline.terminate_nodes()
            print("Pipeline Terminated")
        else:
            print("Error: pipeline not running")

    def get_graph_prefix(self):
        return f"/{self.name}"

    def run(self):
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        
        def _kill_webserver(sig, frame):
            print("\nCTRL+C Caught!; Killing Webserver...")
            self.server.should_exit = True
            self.fastapi_thread.join()
            print("Webserver Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        self.fastapi_thread.start()

        # launch the pipeline
        print("Launching Pipeline...")
        self.pipeline.launch_nodes()