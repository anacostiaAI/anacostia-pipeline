### Test Objective:
Show pipeline can trigger properly when the resource node is on the successor pipeline.

### Pipeline Configuration:
Predecessor pipeline:
- Running on https://127.0.0.1:8000
- Nodes: metadata store node running on https://127.0.0.1:8000/metadata_store

Successor pipeline:
- Running on https://127.0.0.1:8001
- Nodes: 
    - Resource node running on https://127.0.0.1:8001/leaf_data_node
    - Action node running on https://127.0.0.1:8001/shakespeare_eval

### Pipeline Trigger:
Files will be created and dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder. `leaf_data_node` will monitor the folder and trigger pipeline upon new files being dumped into the folder.
