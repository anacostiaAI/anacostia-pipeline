import os
import sys
import signal
from importlib import metadata
from threading import Thread
import uvicorn

from jinja2.filters import FILTERS
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from .engine.base import NodeModel, BaseMetadataStoreNode, BaseResourceNode, BaseActionNode
from .engine.pipeline import Pipeline, PipelineModel



PACKAGE_NAME = "anacostia_pipeline"
PACKAGE_DIR = os.path.dirname(sys.modules[PACKAGE_NAME].__file__)

# Additional Filters for Jinja Templates
def basename(value, attribute=None):
    return os.path.basename(value)

def type_name(value):
    return type(value).__name__

FILTERS['basename'] = basename
FILTERS['type'] = type_name



class Webserver(FastAPI):
    def __init__(self, pipeline: Pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.static_dir = os.path.join(PACKAGE_DIR, "static")
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")

        self.mount("/static", StaticFiles(directory=self.static_dir), name="static")
        self.templates = Jinja2Templates(directory=self.templates_dir)

        @self.get('/api/')
        def welcome():
            return {
                "package": PACKAGE_NAME,
                "version": metadata.version(PACKAGE_NAME)
            }

        @self.get('/api/pipeline/')
        def pipeline() -> PipelineModel:
            '''
            Returns information on the pipeline
            '''
            return self.pipeline.model()

        @self.get('/', response_class=HTMLResponse)
        async def hello(request: Request):
            frontend_json = self.__frontend_json()
            nodes = frontend_json["nodes"]
            node_headers = self.__headers()
            return self.templates.TemplateResponse(
                "index.html", 
                {"request": request, "nodes": nodes, "json_data": frontend_json, "header_templates": node_headers}
            )

    def __headers(self):
        node_headers = []
        for node in self.pipeline.nodes:
            header_template = node.get_router().get_header_template()
            if header_template is not None:
                node_headers.append(header_template)
        return node_headers

    def __frontend_json(self):
        model = self.pipeline.model().model_dump()
        edges = []
        for node_model, node in zip(model["nodes"], self.pipeline.nodes):
            node_model["id"] = node_model["name"]
            node_model["label"] = node_model.pop("name")
            node_model["endpoint"] = node.get_router().get_endpoint()
            node_model["status_endpoint"] = node.get_router().get_status_endpoint()
            node_model["work_endpoint"] = node.get_router().get_work_endpoint()

            edges_from_node = [
                { "source": node_model["id"], "target": successor, "endpoint": f"/edge/{node_model['id']}/{successor}" } 
                for successor in node_model["successors"]
            ]
            edges.extend(edges_from_node)

        model["edges"] = edges
        return model


    
def run_background_webserver(pipeline: Pipeline, **kwargs):
    app = Webserver(pipeline)

    for node in pipeline.nodes:
        node_router = node.get_router()
        # Note: we can also use app.mount() to mount the router of each node like so:
        # this might be important if we want to allow the user to specify an app instead of a router
        app.mount(node_router.get_prefix(), node_router)
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

    # launch the pipeline
    print("Launching Pipeline...")
    pipeline.launch_nodes()

    # register the kill handler for the webserver
    signal.signal(signal.SIGINT, _kill_webserver)
    fastapi_thread.start()

