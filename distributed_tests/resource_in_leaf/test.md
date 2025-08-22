### Test Objective:
Show pipeline can trigger properly when the resource node is on the successor pipeline.

### Pipeline Configuration:
Predecessor pipeline:
- Running on https://127.0.0.1:8000
- Nodes:
    - `metadata_store`
        - Type: `SQLiteMetadataStoreNode`
        - Running on https://127.0.0.1:8000/metadata_store
        - Purpose: to provide a central metadata store between the predecessor and successor pipeline.
        - Database location: `./root-artifacts/input_artifacts/metadata_store/metadata.db`
        - Remote successors: `leaf_data_node`

Successor pipeline:
- Running on https://127.0.0.1:8001
- Nodes: 
    - `leaf_data_node`
        - Type: `FilesystemStoreNode`
        - Running on https://127.0.0.1:8001/leaf_data_node
        - Storage directory: `./leaf-artifacts/input_artifacts/shakespeare`
        - Purpose: to detect incoming files dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder and to trigger the pipeline.
        - Successors: `shakespeare_eval`
    - `shakespeare_eval`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/shakespeare_eval
        - Purpose: only here to provide a placeholder.
- Clients:
    - `metadata_store_rpc`
        - Type: `SQLMetadataStoreClient`
        - Running on https://127.0.0.1:8001/metadata_store_rpc/api/client
        - Purpose: to provide an interface with the `metadata_store` on the predecessor pipeline.

### Test Setup:
Create storage directory folders. `leaf_pipeline` starts running first. `root_pipline` starts running. `metadata_store_rpc` server connects with `metadata_store`.

### Pipeline Trigger:
Files will be created and dumped into the `./leaf-artifacts/input_artifacts/shakespeare` folder. `leaf_data_node` will monitor the folder and trigger pipeline upon new files being dumped into the folder.

### Pipeline operation:
Files created by `create_files_1.py` are then used for the first run.

### Instructions to run test:
Run `run_test.sh` file to automatically run tests.