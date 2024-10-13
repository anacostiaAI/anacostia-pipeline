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
from anacostia_pipeline.dashboard.subapps.network import ReceiverNodeApp, SenderNodeApp
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline, PipelineModel
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.utils.constants import Work



PACKAGE_NAME = "anacostia_pipeline"
DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline.dashboard"].__file__)



class RootPipelineApp(FastAPI):
    def __init__(self, name: str, pipeline: RootPipeline, host="127.0.0.1", port=8000, logger: Logger = None, *args, **kwargs):

        @asynccontextmanager
        async def lifespan(app: RootPipelineApp):
            app.log(f"Opening client for root pipeline '{app.name}'", level="INFO")

            yield

            for route in app.routes:
                if isinstance(route, Mount):
                    if isinstance(route.app, BaseNodeApp):
                        app.log(f"Closing client for subapp '{route.path}'", level="INFO")
                        subapp: BaseNodeApp = route.app
                        await subapp.client.aclose()

            app.log(f"Closing client for root pipeline '{app.name}'", level="INFO")
            await app.client.aclose()

        super().__init__(lifespan=lifespan, *args, **kwargs)
        self.name = name
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.logger = logger
        self.client = httpx.AsyncClient()

        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

        for node in self.pipeline.nodes:
            node_subapp: BaseNodeApp = node.get_app()
            self.mount(node_subapp.get_node_prefix(), node_subapp)       # mount the BaseNodeApp to PipelineWebserver

        @self.get('/api/')
        def welcome():
            return {
                "package": PACKAGE_NAME,
                "version": metadata.version(PACKAGE_NAME)
            }

        @self.get('/api/pipeline/')
        def pipeline_api() -> PipelineModel:
            '''
            Returns information on the pipeline
            '''
            return self.pipeline.model()

        @self.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            frontend_json = self.__frontend_json()
            nodes = frontend_json["nodes"]
            return index_template(nodes, frontend_json, "/graph_sse")

        @self.get("/header_bar", response_class=HTMLResponse)
        def header_bar(node_id: str, visibility: bool = False):
            html_responses = []
            frontend_json = self.__frontend_json()

            node_models = frontend_json["nodes"]
            for node_model in node_models:
                if node_model["id"] != node_id:
                    snippet = node_bar_invisible(node_model=node_model)
                else:
                    if visibility is False:
                        snippet = node_bar_closed(node_model=node_model, open_div_endpoint=f'/header_bar/?node_id={node_model["id"]}&visibility=true')
                    else:
                        snippet = node_bar_open(node_model=node_model, close_div_endpoint=f'/header_bar/?node_id={node_model["id"]}&visibility=false') 

                html_responses.append(snippet)

            return "\n".join(html_responses)
            
        @self.get('/graph_sse', response_class=StreamingResponse)
        async def graph_sse(request: Request):
            edge_color_table = {}
            for node in self.pipeline.nodes:
                for successor in node.successors:
                    edge_color_table[f"{node.name}_{successor.name}"] = None

            async def event_stream():
                while True:
                    try:
                        for node in self.pipeline.nodes:
                            for successor in node.successors:
                                edge_name = f"{node.name}_{successor.name}"

                                if Work.WAITING_SUCCESSORS in node.work_list:
                                    if edge_color_table[edge_name] != "red":
                                        yield f"event: {edge_name}_change_edge_color\n"
                                        yield f"data: red\n\n"
                                        edge_color_table[edge_name] = "red"
                                else: 
                                    if edge_color_table[edge_name] != "black":
                                        yield f"event: {edge_name}_change_edge_color\n"
                                        yield f"data: black\n\n"
                                        edge_color_table[edge_name] = "black"
                                
                        await asyncio.sleep(0.1)

                    except asyncio.CancelledError:
                        print("event source /graph_sse closed")
                        yield "event: close\n"
                        yield "data: \n\n"
                        break

            return StreamingResponse(event_stream(), media_type="text/event-stream")

    def __frontend_json(self):
        model = self.pipeline.model().model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            node_model["id"] = node_model["name"]
            # label is for creating a more readable name, in the future, enable users to input their own labels
            node_model["label"] = node_model["name"].replace("_", " ")
            node_model["endpoint"] = node.get_app().get_endpoint()
            node_model["status_endpoint"] = node.get_app().get_status_endpoint()
            node_model["work_endpoint"] = node.get_app().get_work_endpoint()
            node_model["header_bar_endpoint"] = f'''/header_bar/?node_id={node_model["id"]}'''

            edges_from_node = [
                { 
                    "source": node_model["id"], "target": successor, 
                    "event_name": f"{node_model['id']}_{successor}_change_edge_color" 
                } 
                for successor in node_model["successors"]
            ]
            edges.extend(edges_from_node)

        model["edges"] = edges
        return model
    
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

    def get_graph_prefix(self):
        return f"/{self.name}"

    def run(self):
        config = uvicorn.Config(self, host=self.host, port=self.port)
        server = uvicorn.Server(config)
        fastapi_thread = Thread(target=server.run, name=self.name)

        original_sigint_handler = signal.getsignal(signal.SIGINT)
        
        def _kill_webserver(sig, frame):
            print("\nCTRL+C Caught!; Killing Webserver...")
            server.should_exit = True
            fastapi_thread.join()
            print("Webserver Killed...")

            print("Killing pipeline...")
            self.pipeline.terminate_nodes()
            print("Pipeline Killed.")

            # register the original default kill handler once the pipeline is killed
            signal.signal(signal.SIGINT, original_sigint_handler)

        # register the kill handler for the webserver
        signal.signal(signal.SIGINT, _kill_webserver)
        fastapi_thread.start()

        # launch the pipeline
        print("Launching Pipeline...")
        self.pipeline.launch_nodes()
