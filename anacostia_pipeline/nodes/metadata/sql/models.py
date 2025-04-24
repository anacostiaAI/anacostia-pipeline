from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Text, Table
from sqlalchemy.orm import declarative_base, relationship



Base = declarative_base()


class Run(Base):
    __tablename__ = 'runs'
    
    run_id = Column(Integer, primary_key=True)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)

    # Optional: add relationships if needed
    metrics = relationship("Metric", back_populates="run")
    tags = relationship("Tag", back_populates="run")
    params = relationship("Param", back_populates="run")
    artifacts = relationship("Artifact", back_populates="run")


class Node(Base):
    __tablename__ = 'nodes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(String)
    node_type = Column(String)
    init_time = Column(DateTime)

    # Optional relationships
    metrics = relationship("Metric", back_populates="node")
    tags = relationship("Tag", back_populates="node")
    params = relationship("Param", back_populates="node")
    artifacts = relationship("Artifact", back_populates="node")
    triggers = relationship("Trigger", back_populates="node")


class Metric(Base):
    __tablename__ = 'metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('runs.run_id'))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    metric_name = Column(String)
    metric_value = Column(Float)

    run = relationship("Run", back_populates="metrics")
    node = relationship("Node", back_populates="metrics")


class Param(Base):
    __tablename__ = 'params'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('runs.run_id'))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    param_name = Column(String)
    param_value = Column(String)

    run = relationship("Run", back_populates="params")
    node = relationship("Node", back_populates="params")


# Association table for many-to-many relationship between artifacts and tags to allow for tagging of artifacts
artifact_tags = Table(
    'artifact_tags',
    Base.metadata,
    Column('artifact_id', Integer, ForeignKey('artifacts.id'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tags.id'), primary_key=True)
)


class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('runs.run_id'))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    tag_name = Column(String)
    tag_value = Column(String)

    run = relationship("Run", back_populates="tags")
    node = relationship("Node", back_populates="tags")

    # back-reference to artifacts
    artifacts = relationship("Artifact", secondary="artifact_tags", back_populates="tags")


class Artifact(Base):
    __tablename__ = 'artifacts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('runs.run_id'), nullable=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    location = Column(String)
    created_at = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    state = Column(String, default='new')
    hash = Column(String, nullable=True)
    hash_algorithm = Column(String, nullable=True)
    size = Column(Integer, nullable=True)
    content_type = Column(String, nullable=True)

    run = relationship("Run", back_populates="artifacts")
    node = relationship("Node", back_populates="artifacts")

    # tags on this artifact
    tags = relationship("Tag", secondary="artifact_tags", back_populates="artifacts")


class Trigger(Base):
    __tablename__ = 'triggers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_triggered = Column(Integer, nullable=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    trigger_time = Column(DateTime)
    message = Column(Text, nullable=True)

    node = relationship("Node", back_populates="triggers")
