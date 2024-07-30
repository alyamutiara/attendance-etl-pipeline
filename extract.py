import pandas as pd
import psycopg2
from psycopg2 import Error
from config import Config
import os

class Extractor:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect_to_db()
        print("Starting extraction...")
    
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
        
    def ingest_csv_to_table(self, csv_file, table_name):
        """Read CSV file and insert data into the PostgreSQL table"""
        if not self.connection or not self.cursor:
            print("No database connection available.")
            return
        
        try:
            # Read the CSV file to a dataframe
            df = pd.read_csv(csv_file)

            # Check if the ID already exist in the table
            for row in df.itertuples(index=False):
                id_value = row.ID

                # Check whether ID is already exist in the table or not
                self.cursor.execute(f"SELECT 1 FROM {table_name} WHERE id = %s;", (id_value,))
                exists = self.cursor.fetchone()

                if not exists:
                    # Only add new one that the ID is not exist in the table
                    columns = ', '.join(df.columns)
                    values = ', '.join(['%s'] * len(df.columns))
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

                    self.cursor.execute(insert_query, row)

            self.connection.commit()
            print(f"Data from {csv_file} inserted into {table_name} successfully.")

        except (Exception, Error) as error:
            print(f"Error while inserting data from {csv_file}: {error}")
            self.connection.rollback()

    def process_all_csv_files(self):
        """Process all CSV files in the source directory"""
        for file_name in os.listdir(Config.SOURCE_PATH):
            if file_name.endswith('.csv'):
                file_path = os.path.join(Config.SOURCE_PATH, file_name)
                table_name = f"stg__{os.path.splitext(file_name)[0]}"
                self.ingest_csv_to_table(file_path, table_name)
        
    
    def run(self):
        ddl_file_path = 'stg_ddl.sql'
        self.execute_ddl_from_file(ddl_file_path)
        self.process_all_csv_files()
        self.close_db()
        print("Extract process is finished.")
        print("------------------------------------------")