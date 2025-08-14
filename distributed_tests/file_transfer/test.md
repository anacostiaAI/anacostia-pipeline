### Test Objective:
Show pipelines can transfer files between each other. 

### Pipeline Configuration:
Predecessor pipeline (root.py):
- Name: `root_pipeline`
- Running on https://127.0.0.1:8000
- Nodes: 
    - `metadata_store`:
        - Type: `SQLiteMetadataStoreNode`
        - Server running on https://127.0.0.1:8000/metadata_store
        - Purpose: keep metadata for runs
        - Database location: `./root-artifacts/input_artifacts/metadata_store/metadata.db`
        - Successors: `haiku_data_store`, `model_registry`, `plots_store`
    - `haiku_data_store`
        - Type: `FilesystemStoreNode`
        - Server running on https://127.0.0.1:8000/haiku_data_store
        - Purpose: watch for new files and trigger pipeline
        - Storage directory: `./root-artifacts/input_artifacts/haiku`
        - Successors: `retraining`
    - `model_registry`
        - Type: `FilesystemStoreNode`
        - Server running on https://127.0.0.1:8000/model_registry
        - Purpose: store files created by `retraining` node
        - Storage directory: `./root-artifacts/output_artifacts/model_registry`
        - Successors: `retraining`
    - `plots_store`
        - Type: `FilesystemStoreNode`
        - Server running on https://127.0.0.1:8000/plots_store
        - Purpose: store files uploaded by `haiku_eval`
        - Storage directory: `./root-artifacts/output_artifacts/plots`
        - Successors: `retraining`
    - `retraining`
        - Type: `BaseActionNode`
        - Server running on https://127.0.0.1:8001/retraining
        - Purpose: load in artifacts from `haiku_data_store` and use it to simulate retraining of new models
        - Remote successors: `shakespeare_eval`, `haiku_eval`

Successor pipeline (leaf.py):
- Name: `leaf_pipeline`
- Running on https://127.0.0.1:8001
- Nodes: 
    - `shakespeare_eval`
        - Type: `BaseActionNode`
        - Server running on https://127.0.0.1:8001/shakespeare_eval
        - Purpose: use `model_registry_rpc` to download files from `model_registry`
    - `haiku_eval`
        - Type: `BaseActionNode`
        - Server running on https://127.0.0.1:8001/haiku_eval
        - Purpose: use `plots_store_rpc` to upload files to `plots_store`
- Clients:
    - `metadata_store_rpc`
        - Type: `SQLMetadataStoreClient`
        - Running on https://127.0.0.1:8001/metadata_store_rpc/api/client
        - Purpose: to enable `haiku_eval` and `shakespeare_eval` to get the run ID from the metadata store
    - `model_registry_rpc`
        - Type: `FilesystemStoreClient`
        - Running on https://127.0.0.1:8001/model_registry_rpc/api/client
        - Purpose: to enable `shakespeare_eval` to download files from `model_registry` over the network
        - Storage directory: `./leaf-artifacts/input_artifacts/model_registry`
    - `plots_store_rpc`
        - Type: `FilesystemStoreClient`
        - Running on https://127.0.0.1:8001/plots_store_rpc/api/client
        - Purpose: to enable `haiku_eval` to upload files to `plots_store` over the network
        - Storage directory: `./leaf-artifacts/output_artifacts/plots`

### Test Setup:
Create storage directory folders. `leaf_pipeline` starts running first. `root_pipline` starts running. `metadata_store_rpc` server connects with `metadata_store`, `model_registry` server connects with `model_registry_rpc`, `plots_store` server connects with `plots_store_rpc`.

### Pipeline Trigger:
Files will be created and dumped into the `./root-artifacts/input_artifacts/haiku` folder. `haiku_data_store` will monitor the folder and trigger pipeline upon new files being dumped into the folder.

### Pipeline operation:
Upon being triggered by `haiku_data_store`, `metadata_store` will start a new run, the `retraining` node will save an artifact with a name like `model0.txt` to `model_registry` and signal `shakespeare_eval` and `haiku_eval` over the network. `shakespeare_eval` will download the model from `model_registry`, meanwhile `haiku_eval` will upload an artifact with a name like `plot0.txt` to the `plots_store`. 

### Instructions to run test:
Run `run_test.sh` file to automatically run tests.
To run tests manually:
1. Open up three terminals
2. Run `python setup.py` in terminal 1
3. Run `python leaf.py "127.0.0.1" 8001` in terminal 2
4. Run `python root.py "127.0.0.1" 8000 "127.0.0.1" 8001` in terminal 3
5. Go back to terminal 1 and run `python create_files.py`
6. Open up a browser and navigate to https://127.0.0.1:8000 to see the GUI
