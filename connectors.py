import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_HOST = "172.31.144.1"
MYSQL_PORT = "3306"
MYSQL_DB = "sakila"

MYSQL_URI = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
SQLITE_URI = "sqlite:///sakila_analytics.db"

mysql_engine = create_engine(MYSQL_URI, echo=False)
sqlite_engine = create_engine(SQLITE_URI, echo=False)
MySQLSession = sessionmaker(autocommit=False, autoflush=False, bind=mysql_engine)
SQLiteSession = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)

def get_mysql_session():
    """MySQL session handler."""
    session = MySQLSession()
    try:
        yield session
    finally:
        session.close()

def get_sqlite_session():
    """SQLITE session handler."""
    session = SQLiteSession()
    try:
        yield session
    finally:
        session.close()