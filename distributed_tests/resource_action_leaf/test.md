### Objective:
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
