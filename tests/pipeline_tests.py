import time
from typing import List
import sys

sys.path.append("..")
sys.path.append("../anacostia_pipeline")
from anacostia_pipeline.engine.base import BaseActionNode, BaseResourceNode, BaseNode
from anacostia_pipeline.engine.pipeline import Pipeline


class DataStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)
    
    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")
    
    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class FeatureStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class ArtifactStoreNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class ModelRegistryNode(BaseResourceNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name, uri)

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def update_state(self):
        print(f"Updating state of {self.name}")
        time.sleep(1)
        print(f"Finished updating state of {self.name}")

class AndNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode]) -> None:
        super().__init__(name, predecessors)
    
    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class DataPrepNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        data_store: DataStoreNode, 
        feature_store: FeatureStoreNode, 
        and_node: AndNode
    ) -> None:
        super().__init__(name=name, predecessors=[data_store, feature_store, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class RetrainingNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        feature_store: FeatureStoreNode, 
        model_registry: ModelRegistryNode, 
        data_prep: DataPrepNode,
        and_node: AndNode
    ) -> None:
        super().__init__(name, predecessors=[feature_store, data_prep, model_registry, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class ModelEvaluationNode(BaseActionNode):
    def __init__(self, name: str, model_registry: ModelRegistryNode, and_node: AndNode) -> None:
        super().__init__(name, predecessors=[model_registry, and_node])
    
    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")
    
    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True

class ReportingNode(BaseActionNode):
    def __init__(self, name: str, artifact_store: ArtifactStoreNode, retraining_node: RetrainingNode, and_node: AndNode) -> None:
        super().__init__(name, predecessors=[artifact_store, retraining_node, and_node])

    def setup(self) -> None:
        print(f"Setting up {self.name}")
        time.sleep(1)
        print(f"Finished setting up {self.name}")

    def execute(self):
        print(f"Executing {self.name}")
        time.sleep(1)
        print(f"Finished executing {self.name}")
        return True


if __name__ == "__main__":
    data_store_node = DataStoreNode("data store", "http://localhost:8080")
    feature_store_node = FeatureStoreNode("feature store", "http://localhost:8080")
    artifact_store_node = ArtifactStoreNode("artifact store", "http://localhost:8080")
    model_registry_node = ModelRegistryNode("model registry", "http://localhost:8080")
    and_node1 = AndNode("AND node 1", [data_store_node, feature_store_node, artifact_store_node, model_registry_node])
    #and_node2 = AndNode("AND node 2", [and_node1])
    #and_node3 = AndNode("AND node 3", [and_node1])
    data_prep_node = DataPrepNode("data prep", data_store_node, feature_store_node, and_node1)
    retraining_node = RetrainingNode("retraining", feature_store_node, model_registry_node, data_prep_node, and_node1)
    reporting_node = ReportingNode("reporting", artifact_store_node, retraining_node, and_node1)
    model_evaluation_node = ModelEvaluationNode("model evaluation", model_registry_node, and_node1)

    pipeline = Pipeline(
        [
            feature_store_node, artifact_store_node, model_registry_node, data_store_node,  
            retraining_node, data_prep_node, reporting_node, model_evaluation_node, 
            and_node1, #and_node2, and_node3
        ]
    )

    #print(pipeline.nodes)
    #print(retraining_node.predecessors)
    #print(model_registry_node.successors)

    pipeline.launch_nodes()
    time.sleep(20)
    pipeline.terminate_nodes()