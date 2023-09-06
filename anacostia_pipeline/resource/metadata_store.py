from anacostia_pipeline.engine.node import ResourceNode
from sqlalchemy import create_engine, Integer, String, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, Session


class Base(DeclarativeBase):
    pass


class MetadataTable(Base):
    __tablename__ = "metadata"

    # these fields are good
    id = mapped_column(Integer, primary_key=True)
    train_acc = mapped_column(Float)
    valid_acc = mapped_column(Float)
    test_acc = mapped_column(Float)
    epoch = mapped_column(Integer)
    model_path = mapped_column(String)
    features_path = mapped_column(String)
    data_path = mapped_column(String)
    state = mapped_column(String)


class MetadataStoreNode(ResourceNode):
    '''A ResourceNode responsible for saving training and model evaluation data into a persistant store'''
    
    def __init__(self, name: str, path: str = "sqlite:///default_metadata.db"):
        self.engine = create_engine(path)

        # Automatically create the table if it doesn't exist
        Base.metadata.create_all(self.engine)
        super().__init__(name, "metadata_store")

    @ResourceNode.resource_accessor
    def setup(self) -> None:
        # TODO: implement a function that installs the database, 
        # spin up the database, 
        # and opens a connection to the database when the node is started
        pass
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def insert_metadata(self, **kwargs):
        # TODO: implement a function that inserts the metadata into the database
        # users will use kwargs to pass in the metadata
        pass

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        raise NotImplementedError
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def get_metadata(self, state: str) -> iter:
        if state not in ["current", "old", "new", "all"]:
            raise ValueError("state must be one of ['current', 'old', 'new', 'all']")
        
        # TODO: implement functions for retrieveing metadata from the database based on the state
        # Note: this function should be an iterator, see load_data_samples and load_data_samples in data_store.py
        pass

    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        # TODO: implement a function that updates the state of the metadata store
        # i.e., move the current metadata to old, and the new metadata to current
        return True

    def on_exit(self) -> None:
        # TODO: implement a function that closes the database connection when the node is terminated
        pass