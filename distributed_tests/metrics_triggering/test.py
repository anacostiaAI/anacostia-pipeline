import os
import shutil
import logging
from logging.config import dictConfig
from typing import List

from loggers import ROOT_ACCESS_LOGGING_CONFIG, ROOT_ANACOSTIA_LOGGING_CONFIG
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer

# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")



class MetricMonitoringNode(SQLiteMetadataStoreNode):
    def __init__(self, name, uri, remote_successors = None, client_url = None, loggers = None):
        super().__init__(name, uri, remote_successors, client_url, loggers)
    
    def metadata_store_trigger(self) -> None:
        # get the highest accuracy for this run
        run_id = self.get_run_id()
        node_name = "edge_deployment"
        metrics = self.get_metrics(node_name=node_name, run_id=run_id)
        accuracy_scores = [metric['metric_value'] for metric in metrics if metric["metric_name"] == "percent_accuracy"]
        highest_accuracy = max(accuracy_scores)

        # trigger condition
        if highest_accuracy > 0.4:
            self.trigger(f"% accuracy = {highest_accuracy}, trigger condition % accuracy > 0.4 satisfied")



# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class PrintingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None) -> None:
        super().__init__(name=name, predecessors=predecessors)
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Logging node executed", level="INFO")
        return True

# Create the nodes
metadata_store = MetricMonitoringNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store, monitoring=False)
printing_node = PrintingNode("logging_node", predecessors=[data_store])

# Create the pipeline
pipeline = RootPipeline(nodes=[metadata_store, data_store, printing_node], loggers=logger)

# Create the web server
webserver = RootPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
webserver.run()