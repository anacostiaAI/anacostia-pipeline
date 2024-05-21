import os
import sys
import signal
from importlib import metadata
from threading import Thread
import uvicorn
import asyncio

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse

from .components.index import index_template

from ..engine.pipeline import Pipeline, PipelineModel
from ..engine.constants import Work



PACKAGE_NAME = "anacostia_pipeline"
DASHBOARD_DIR = os.path.dirname(sys.modules["anacostia_pipeline.dashboard"].__file__)

# Additional Filters for Jinja Templates
def basename(value, attribute=None):
    return os.path.basename(value)

def type_name(value):
    return type(value).__name__



class Webserver(FastAPI):
    def __init__(self, pipeline: Pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.static_dir = os.path.join(DASHBOARD_DIR, "static")
        self.mount("/static", StaticFiles(directory=self.static_dir), name="webserver")

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
            node_headers = self.__headers()
            return index_template(nodes, frontend_json, "/graph_sse", node_headers)

        @self.get('/graph_sse', response_class=StreamingResponse)
        async def graph_sse(request: Request):
            async def event_stream():
                while True:
                    try:
                        for node in self.pipeline.nodes:
                            for successor in node.successors:
                                if Work.WAITING_SUCCESSORS in node.work_list:
                                    yield f"event: {node.name}_{successor.name}_change_edge_color\n"
                                    yield f"data: red\n\n"
                                else: 
                                    yield f"event: {node.name}_{successor.name}_change_edge_color\n"
                                    yield f"data: black\n\n"
                        await asyncio.sleep(1)

                    except asyncio.CancelledError:
                        print("event source closed")
                        yield "event: close\n"
                        yield "data: \n\n"
                        break

            return StreamingResponse(event_stream(), media_type="text/event-stream")
            
    def __headers(self):
        node_headers = []
        for node in self.pipeline.nodes:
            # change .get_header_template() to .get_headers)
            header_template = node.get_app().get_header_template()
            if header_template is not None:
                node_headers.append(header_template)
        return node_headers

    def __frontend_json(self):
        model = self.pipeline.model().model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            node_model["id"] = node_model["name"]
            node_model["label"] = node_model.pop("name")
            node_model["endpoint"] = node.get_app().get_endpoint()
            node_model["status_endpoint"] = node.get_app().get_status_endpoint()
            node_model["work_endpoint"] = node.get_app().get_work_endpoint()

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


    
def run_background_webserver(pipeline: Pipeline, **kwargs):
    app = Webserver(pipeline)

    for node in pipeline.nodes:
        node_subapp = node.get_app()
        app.mount(node_subapp.get_prefix(), node_subapp)
        # app.include_router(node_router, prefix=node_router.get_prefix())

    config = uvicorn.Config(app, **kwargs)
    server = uvicorn.Server(config)
    fastapi_thread = Thread(target=server.run)

    original_sigint_handler = signal.getsignal(signal.SIGINT)

    def _kill_pipeline(sig, frame):
        print("\nCTRL+C Caught!; Killing Pipeline...")
        pipeline.terminate_nodes()

        # register the original default kill handler once the pipeline is killed
        signal.signal(signal.SIGINT, original_sigint_handler)
        print("Pipeline Killed.")
    
    def _kill_webserver(sig, frame):
        print("\nCTRL+C Caught!; Killing Webserver...")
        server.should_exit = True
        fastapi_thread.join()

        # register the kill handler for the pipeline once the webserver is killed
        signal.signal(signal.SIGINT, _kill_pipeline)
        print("Webserver Killed; press CTRL+C again to kill pipeline...")

    # register the kill handler for the webserver
    signal.signal(signal.SIGINT, _kill_webserver)
    fastapi_thread.start()

    # launch the pipeline
    print("Launching Pipeline...")
    pipeline.launch_nodes()
