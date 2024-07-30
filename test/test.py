import pandas as pd
import psycopg2
from psycopg2 import Error
from config import Config

class Transformer:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.dataframes = {}
        self.connect_to_db()
        print("Starting transformation...")

    def connect_to_db(self):
        """Establish a connection to the PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_SCHEMA,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD
            )
            self.cursor = self.connection.cursor()
            print("Database connection established.")

        except(Exception, Error) as error:
            print(f"Error while connecting to PostgreSQL: {error}")
            self.connection = None
            self.cursor = None

    def close_db(self):
        """Close the database connection and cursor"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection is closed.")

    def get_staging_tables(self):
        """Retrieve list of staging tables"""
        self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'stg__%'")
        tables = self.cursor.fetchall()
        return [table[0] for table in tables]

    def transform_and_load(self):
        """Fetch data from staging tables, trasnform it, then load to the data warehouse"""
        try:
            staging_tables = self.get_staging_tables()
            for table in staging_tables:
                df = pd.read_sql_query(f"SELECT * FROM {table}", self.connection)

                # Perform data transformation
                transformed_df = self.transform_data(df, table)

                # Ingest transformed data into the data warehouse layer
                datawarehouse_table = f"wh_{table[4:]}"
                self.ingest_transformed_data(transformed_df, datawarehouse_table)
            print("Data transformation and loading completed.")
        except(Exception, Error) as error:
            print(f"Error during data transformation and loading: {error}")
            self.connection.rollback()
    
    def transform_data(self, df, table_name):
        """Data transformation and manipulation"""
        if table_name == "stg__schedules":
            df['start_dt'] = pd.to_datetime(df['start_dt'], format='%Y-%m-%d', errors='coerce')
            df['end_dt'] = pd.to_datetime(df['end_dt'], format='%Y-%m-%d', errors='coerce')
        elif table_name == 'stg__enrollments':
            df['enroll_dt'] = pd.to_datetime(df['enroll_dt'], format='%Y-%m-%d', errors='coerce')
        elif table_name == 'stg__attendances':
            df['attend_dt'] = pd.to_datetime(df['attend_dt'], format='%Y-%m-%d', errors='coerce')
        else:
            df = df.drop_duplicates(subset='id')
        return df
    
    def execute_ddl_from_file(self, file_path):
        """Execute DDL statements from a file"""
        if not self.connection or not self.cursor:
            print("No database connection available.")
            return

        try:
            with open(file_path, 'r') as file:
                ddl_statements = file.read().split(';')
            
            for statement in ddl_statements:
                if statement.strip():
                    self.cursor.execute(statement)
                    self.connection.commit()
            print("DDL statements executed successfully.")

        except (Exception, Error) as error:
            print(f"Error while executing DDL statements: {error}")
            self.connection.rollback()
    
    def ingest_transformed_data(self, df, table_name):
        """Insert the transformed data to datawarehouse table"""
        if not self.connection or not self.cursor:
            print("No database connection available.")
            return
        try:
            for row in df.itertuples(index=False):
                id_value = row.id

                # Check whether ID is already exist in the table or not
                self.cursor.execute(f"SELECT 1 FROM {table_name} WHERE id = %s;", (id_value,))
                exists = self.cursor.fetchone()

                if not exists:
                    # Only add new one that the ID is not exist in the table
                    columns = ', '.join(df.columns)
                    values = ', '.join(['%s'] * len(df.columns))
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

                    self.cursor.execute(insert_query, tuple(row))

            self.connection.commit()
            print(f"Data loaded into {table_name} successfuly.")

        except(Exception, Error) as error:
            print(f"Error while inserting transformed data into {table_name}: {error}")
            self.connection.rollback()
        
    def run(self):
        ddl_file_path = "wh_ddl.sql"
        self.execute_ddl_from_file(ddl_file_path)
        self.transform_and_load()
        self.close_db()
        print("Transformation process is finished.")
        print("------------------------------------------")