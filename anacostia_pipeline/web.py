from importlib import metadata

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import uvicorn

from anacostia_pipeline.engine.base import NodeModel
from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel

PACKAGE_NAME="anacostia_pipeline"

class Webserver:
    def __init__(self, p:Pipeline):
        self.p = p
        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory="./static"), name="static")
        self.templates = Jinja2Templates(directory="./templates")

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

        @self.app.get('/', response_class=HTMLResponse)
        async def index(request: Request):
            nodes = self.p.model().nodes
            return self.templates.TemplateResponse("base.html", {"request": request, "nodes": nodes, "title": "Anacostia Pipeline"})

        @self.app.get('/node', response_class=HTMLResponse)
        async def node(request: Request, name:str, property:str=None):
            n = self.p[name].model()
            if n is None:
                return "<h1>Node Not Found</h1>"
            
            if property is None:
                return n.view(self.templates, request)

            return str(getattr(n, property))

    def run(self):
        uvicorn.run(self.app, host="0.0.0.0", port=8000)