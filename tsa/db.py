import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

database_uri = os.environ.get("DB", "sqlite:///project.db")
engine = create_engine(database_uri, pool_recycle=3600, pool_size=10)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)
