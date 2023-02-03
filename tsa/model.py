from tsa.extensions import db
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class Label(Base):
    __tablename__ = 'label'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    iri = db.Column(db.String, nullable=False)
    language_code = db.Column(db.String, nullable=True)
    label = db.Column(db.String, nullable=False)

class DDR(Base):
    __tablename__ = 'ddr'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    relationship_type = db.Column(db.String, nullable=False)
    iri1 = db.Column(db.String, nullable=False)
    iri2 = db.Column(db.String, nullable=False)

class Concept(Base):
    __tablename__ = 'concept'
    iri = db.Column(db.String, primary_key=True)

class RobotsDelay(Base):
    __tablename__ = 'robots_delay'
    iri = db.Column(db.String, primary_key=True)
    expiration = db.Column(db.DateTime, nullable=False)

class DatasetDistribution(Base):
    __tablename__ = 'dataset_distribution'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ds = db.Column(db.String, nullable=False)
    distr = db.Column(db.String, nullable=False)

class DatasetEndpoint(Base):
    __tablename__ = 'dataset_endpoint'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ds = db.Column(db.String, nullable=False)
    endpoint = db.Column(db.String, nullable=False)

class PureSubject(Base):
    __tablename__ = 'pure_subject'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    distribution_iri = db.Column(db.String, nullable=False)
    subject_iri = db.Column(db.String, nullable=False)

class Relationship(Base):
    __tablename__ = 'relationship'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    type = db.Column(db.String, nullable=False)
    group = db.Column(db.String, nullable=False)
    candidate = db.Column(db.String, nullable=False)

Base.registry.configure()
ddr_index = DDR()

