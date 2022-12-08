import os
if os.path.exists('env.py'):
    import env

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine

from typing import Union, Tuple


def connect_to_db() -> Union[Tuple[Connection, Engine], None]:
    """
    Connect to Amazon RDS Postgres Server
    :return: Connection
    """
    host = os.environ.get('DB_HOST')
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASS')
    port = os.environ.get('DB_PORT')

    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'

    engine = create_engine(conn_str)
    conn = engine.connect()

    if conn:
        return conn, engine
    else:
        return None

