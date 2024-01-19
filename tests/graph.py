from logging import Logger
import os
import time
import logging
import shutil
from typing import List, Union

from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.frontend import run_background_webserver

from anacostia_pipeline.resources.filesystem_store import FilesystemStoreNode
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore
from anacostia_pipeline.engine.constants import Work

from utils import *



class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)

    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 1


class ModelRegistryNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)

    def create_filename(self) -> str:
        return f"processed_data_file{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, content: str) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: for monitoring-enabled resource nodes, record_artifact should be called before create_file;
        # that way, the Observer can see the file is already logged and ignore it
        self.record_current(filepath)
        with open(filepath, 'w') as f:
            f.write(content)
        self.log(f"Saved preprocessed {filepath}", level="INFO")


class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)


class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry])
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)

    @BaseNode.update_work_list(Work.EXECUTION)
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'", level="INFO")

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}", level="INFO")
        
        for filepath in self.data_store.list_artifacts("old"):
            self.log(f"Already trained on {filepath}", level="INFO")
        
        self.metadata_store.log_metrics(acc=1.00)
        
        self.metadata_store.log_params(
            batch_size = 64, # how many independent sequences will we process in parallel?
            block_size = 256, # what is the maximum context length for predictions?
            max_iters = 2500,
            eval_interval = 500,
            learning_rate = 3e-4,
            eval_iters = 200,
            n_embd = 384,
            n_head = 6,
            n_layer = 6,
            dropout = 0.2,
            seed = 1337,
            split = 0.9    # first 90% will be train, rest val
        )

        self.metadata_store.set_tags(test_name="Karpathy LLM test")

        self.log(f"Node '{self.name}' executed successfully.", level="INFO")
        return True


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)

    @BaseNode.update_work_list(Work.EXECUTION)
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def setup(self) -> None:
        super().setup()
        time.sleep(2)

    @BaseNode.update_work_list(Work.EXECUTION)
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")
        self.metadata_store.log_metrics(haiku_test_loss=2.43)
        return True



web_tests_path = "./testing_artifacts/frontend"
if os.path.exists(web_tests_path) is True:
    shutil.rmtree(web_tests_path)
os.makedirs(web_tests_path)

log_path = f"{web_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


path = f"{web_tests_path}/webserver_test"
metadata_store_path = f"{path}/metadata_store"
haiku_data_store_path = f"{path}/haiku"
model_registry_path = f"{path}/model_registry"
plots_path = f"{path}/plots"

metadata_store = SqliteMetadataStore(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db"
)
model_registry = ModelRegistryNode(
    "model_registry", 
    model_registry_path, 
    metadata_store
)
plots_store = PlotsStoreNode("plots_store", plots_path, metadata_store)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode("retraining", haiku_data_store, plots_store, model_registry, metadata_store)
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[retraining], metadata_store=metadata_store)
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[retraining], metadata_store=metadata_store)
pipeline = Pipeline(
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, shakespeare_eval, haiku_eval, retraining], 
    loggers=logger
)



if __name__ == "__main__":
    #print(pipeline.frontend_json())
    run_background_webserver(pipeline, host="127.0.0.1", port=8000, reload=True)
