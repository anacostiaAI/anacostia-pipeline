### Test Objective:
Show leaf pipeline can record data entries on its temporary metadata store and then the root pipeline can pull data entries from leaf pipeline's
metadata store and train on previously detected artifacts.

### Pipeline Configuration:
Predecessor pipeline:
- Running on https://127.0.0.1:8000
- Nodes: metadata store node running on https://127.0.0.1:8000/metadata_store

Successor pipeline:
- Running on https://127.0.0.1:8001
- Nodes: 
    - Metadata node (leaf_metadata_store)
    - Resource node running on https://127.0.0.1:8001/leaf_data_node
    - Action node running on https://127.0.0.1:8001/shakespeare_eval

### Pipeline Trigger:
Files will be created and dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder. `leaf_data_node` will monitor the folder and trigger pipeline upon new files being dumped into the folder.
