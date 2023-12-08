from anacostia_pipeline.engine.base import BaseNode, BaseMetadataStoreNode, NodeModel
from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.web import Webserver

n1=BaseMetadataStoreNode("test_node1")
n2=BaseNode("test_node2", predecessors=[n1])
p = Pipeline([n1, n2])
w = Webserver(p)

app = w.app