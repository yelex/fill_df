from constants import HOST, USER, PASSWORD
from sqlalchemy import create_engine

connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:3306"


def get_connection():
    engine = create_engine(connection_string, echo=True)
    return engine.connect()
