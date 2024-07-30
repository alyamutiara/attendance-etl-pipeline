import os

class Config:
    # Directory paths
    SOURCE_PATH = 'source/'

    DB_HOST = os.getenv('DB_HOST', 'psql_db')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_SCHEMA = os.getenv('DB_SCHEMA', 'university_db')
    DB_USER = os.getenv('DB_USER', 'dataengineer')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'secret')