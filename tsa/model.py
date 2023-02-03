from tsa.extensions import db

class Label(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    iri = db.Column(db.String, nullable=False)
    language_code = db.Column(db.String, nullable=True)
    label = db.Column(db.String, nullable=False)

class DDR(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    relationship_type = db.Column(db.String, nullable=False)
    iri1 = db.Column(db.String, nullable=False)
    iri2 = db.Column(db.String, nullable=False)

class Concept(db.Model):
    iri = db.Column(db.String, primary_key=True)

class RobotsDelay(db.Model):
    iri = db.Column(db.String, primary_key=True)
    expiration = db.Column(db.DateTime, nullable=False)

class DatasetDistribution(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ds = db.Column(db.String, nullable=False)
    distr = db.Column(db.String, nullable=False)

class DatasetEndpoint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ds = db.Column(db.String, nullable=False)
    endpoint = db.Column(db.String, nullable=False)

class PureSubject(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    distribution_iri = db.Column(db.String, nullable=False)
    subject_iri = db.Column(db.String, nullable=False)

ddr_index = DDR()

