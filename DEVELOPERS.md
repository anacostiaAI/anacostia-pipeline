## Tips on building your own plugins for Anacostia

### Metadata Store
- Implementing a NoSQL metadata store:
    1. Inherit the ```BaseMetadataStoreNode``` class 
    2. Override all base methods that raise ```NotImplementedError``` with some default code
    3. Start by implementing setup() method
        - Tip: don’t call ```Pipeline.launch_nodes()```, just call ```Pipeline.setup_nodes()```
        - Desired result: verify you can acquire, configure to work with the pipeline, and that the pipeline can be spun up
    4. Implement the ```create_entry, add_node, get_num_entries, entry_exists``` method
    6. Implement the ```start_run()``` and ```end_run()``` methods
    7. Implement the ```add_run_id()``` and ```add_end_time()``` methods
- Implementing a SQL metadata store:
    1. Inherit the ```BaseSQLMetadataStoreNode``` class 
    2. Override the abstract method `setup()` with custom logic for creating a `Session` (see how `SQLiteMetadataStoreNode` creates an engine with `check_same_thread` set to `False`, creates all the tables, uses `sessionmaker` to create a `Session`, and then calls `init_scoped_session` to register the `Session` object with the parent class)

Note: See sql_metadata_store.py for an example of how to create your own metadata store node.

### Resource Node
1. Figure out a way to monitor your resource for incoming changes
2. Inherit the ```BaseResourceNode``` class 
3. Override all base methods that raise ```NotImplementedError``` with some default code
4. Start by implementing setup() method
    - Tip: don’t call ```Pipeline.launch_nodes()```, just call ```Pipeline.setup_nodes()```
    - Desired result: verify you can acquire, configure to work with the pipeline, and that the pipeline can be spun up
5. Implement the ```start_monitoring()``` method
    - Note: you must create a new thread and run your monitoring code there
    - Note: you must implement your triggering logic in the thread that monitors your code 
6. Implement the ```stop_monitoring()``` method
7. Implement the ```record_new()``` method
8. Implement the ```record_current()``` method

Note: See filesystem_store.py for an example of how to create your own resource node.

### Action Node
1. Inherit the ```BaseActionNode``` class 
2. Override the ```execute()``` method in ```BaseActionNode```