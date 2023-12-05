from importlib import metadata

from fastapi import FastAPI

from anacostia_pipeline.engine.base import BaseNode, BaseMetadataStoreNode, NodeModel
from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel

PACKAGE_NAME="anacostia_pipeline"

app = FastAPI()

# For now, Assuming we are running 1 Pipeline
n1=BaseMetadataStoreNode("test_node1")
n2=BaseNode("test_node2", predecessors=[n1])
p = Pipeline([n1, n2])

@app.get('/api')
def welcome():
    return {
        "package": PACKAGE_NAME,
        "version": metadata.version(PACKAGE_NAME)
    }

@app.get('/api/pipeline/')
def pipeline() -> PipelineModel:
    '''
    Returns information on the pipeline
    '''
    return p.model()


@app.get('/api/node/')
def nodes(name: str) -> NodeModel:
    '''
    Returns information on a given Node by name
    '''
    return p[name].model()