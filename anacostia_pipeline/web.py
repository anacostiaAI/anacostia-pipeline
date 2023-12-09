import os
import sys
import json
import signal
from importlib import metadata
from multiprocessing import Process

from jinja2.filters import FILTERS
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

import uvicorn

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

class Webserver:
    def __init__(self, p:Pipeline):
        self.p = p
        self.static_dir = os.path.join(PACKAGE_DIR, "static")
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")

        # Only used for self.run(blocking=False)
        self._proc = None

        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory=self.static_dir), name="static")
        self.templates = Jinja2Templates(directory=self.templates_dir)

        @self.app.get('/api')
        def welcome():
            return {
                "package": PACKAGE_NAME,
                "version": metadata.version(PACKAGE_NAME)
            }

        @self.app.get('/api/pipeline/')
        def pipeline() -> PipelineModel:
            '''
            Returns information on the pipeline
            '''
            return self.p.model()


        @self.app.get('/api/node/')
        def nodes(name: str) -> NodeModel:
            '''
            Returns information on a given Node by name
            '''
            return self.p[name].model()

        @self.app.get('/api/metadata')
        def _metadata():
            data_uri = self.p.metadata_store.uri
            with open(data_uri, 'r') as f:
                data = json.load(f)
            return data

        @self.app.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            nodes = self.p.model().nodes
            return self.templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

        @self.app.get('/node', response_class=HTMLResponse)
        async def node_html(request: Request, name:str, property:str=None):
            node = self.p[name]
            if node is None:
                return "<h1>Node Not Found</h1>"
            
            if property is not None:
                n = node.model().dict()
                return n.get(property, "")

            # TODO update this so all node types have a .model() that
            # returns a pydantic BaseModel Type to streamlize serialization of nodes
            if isinstance(node, BaseMetadataStoreNode):
                return node.html(self.templates, request)

            if isinstance(node, BaseResourceNode):
                return node.html(self.templates, request)

            if isinstance(node, BaseActionNode):
                return node.html(self.templates, request)

            n = node.model()
            return n.view(self.templates, request)

    def run(self, blocking=False):
        '''
        Launches the Webserver (Blocking Call)
        '''
        if blocking:
            uvicorn.run(self.app, host="0.0.0.0", port=8000)
            return None
        
        # Launch webserver as new daemon process 
        def _uvicorn_wrapper():
            uvicorn.run(self.app, host="0.0.0.0", port=8000)
        
        print("Launching Webserver")
        self._proc = Process(target=_uvicorn_wrapper, args=())
        self._proc.start()
        print(f"Webserver PID: {self._proc.pid}")
        
        # create handler from this process to kill self._proc
        def _kill_handler(sig, frame):
            print("CTRL+C Caught!; Killing Webserver...")
            # webserver also catches a sigint causing it to die too
            self._proc.join()
            
        signal.signal(signal.SIGINT, _kill_handler)
        print("CTRL+C to Kill the Webserver; Or send a SIGINT to THIS process")
        return self._proc.pid