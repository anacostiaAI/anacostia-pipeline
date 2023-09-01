from anacostia_pipeline.engine.node import ResourceNode
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.resource.feature_store import FeatureStoreNode
from anacostia_pipeline.resource.data_store import DataStoreNode

from sqlalchemy import create_engine, Integer, String, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, Session

class Base(DeclarativeBase):
    pass

class MetadataTable(Base):
    __tablename__ = "metadata"

    id = mapped_column(Integer, primary_key=True)
    train_acc = mapped_column(Float)
    valid_acc = mapped_column(Float)
    test_acc = mapped_column(Float)
    epoch = mapped_column(Integer)
    model_path = mapped_column(String)
    features_path = mapped_column(String)
    data_path = mapped_column(String)


class MetadataStoreNode(ResourceNode):
    '''A ResourceNode responsible for saving training and model evaluation data into a persistant store'''
    
    def __init__(self, model_reg:ModelRegistryNode, feat_store:FeatureStoreNode, data_store:DataStoreNode, path:str="sqlite:///default_metadata.db"):
        self.engine = create_engine(path)

        # Automatically create the table if it doesn't exist
        Base.metadata.create_all(engine)

        # TODO add as parameters and set here once they are defined
        self.train_node = None
        self.model_eval = None

        self.model_reg = model_reg
        self.feat_store = feat_store
        self.data_store = data_store

    @ResourceNode.lock_decorator
    def insert_metadata(self, train_acc:float=0, valid_acc:float=0, test_acc:float=0, epoch:int=0):
        '''
        Inserts a row into the metadata table
        :param train_acc: Training Accuracy
        :param valid_acc: Validation Accuracy
        :param test_acc: Testing Accuracy
        :param epoch: Number of Epochs that Training had (I Think?)
        '''
        with Session(engine) as s:
            s.add(MetadataTable(
                train_acc=train_acc,
                valid_acc=valid_acc,
                test_acc=test_acc,
                epoch=epoch,
                model_path=self.model_registry_path,
                features_path=self.feat_store.feature_store_path,
                data_path=self.data_store.data_store_path
            ))
        s.commit()


    def execute(self):
        '''
        Returns True if an insertion into the database was successful
        '''
        if not self.train_node:
            return False

        # TODO update once a training node is defined
        epoch = self.train_node.epoch 
        
        if not self.model_eval:
            return False

        # TODO update once a model_eval node is defined
        train_acc = self.model_eval.train_acc
        valid_acc = self.model_eval.valid_acc
        test_acc = self.model_eval.test_acc

        self.insert_metadata(train_acc, valid_acc, test_acc, epoch)
        return True


