import pandas as pd
import psycopg2
from psycopg2 import Error
from config import Config
from datetime import datetime, timedelta
import warnings

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
            df['start_dt'] = pd.to_datetime(df['start_dt'], format="%d-%b-%y", errors='coerce')
            df['end_dt'] = pd.to_datetime(df['end_dt'], format="%d-%b-%y", errors='coerce')

            all_dates = []

            for _, row in df.iterrows():
                start_date = row['start_dt']
                end_date = row['end_dt']
                days_of_week = [int(day) for day in row['course_days'].split(',')]
                current_date = start_date
                while current_date <= end_date:
                    weekday = current_date.weekday()
                    custom_weekday = weekday + 1 % 7 + 1
                    if custom_weekday in days_of_week:
                        all_dates.append({
                            'course_id': row['course_id'],
                            'lecturer_id': row['lecturer_id'],
                            'start_dt': row['start_dt'],
                            'end_dt': row['end_dt'],
                            'course_day': custom_weekday,
                            'schedule_date': (current_date).strftime('%Y-%m-%d'),
                            'week_number': ((current_date - start_date).days // 7) + 1
                        })
                    current_date += timedelta(days=1)

            df = pd.DataFrame(all_dates)
            df.reset_index(drop=True, inplace=True)
            df.index += 1
            df.index.name = 'id'
            df.reset_index(inplace=True)

        elif table_name == 'stg__enrollments':
            df['enroll_dt'] = pd.to_datetime(df['enroll_dt'], format="%d-%b-%y", errors='coerce')

        elif table_name == 'stg__attendances':
            df['attend_dt'] = pd.to_datetime(df['attend_dt'], format="%d-%b-%y", errors='coerce')

        else:
            df = df.drop_duplicates(subset='id')
        return df
    
    def generate_course_dates(self, start_date, end_date, course_days):
        all_dates = pd.date_range(start_date, end_date)
        course_dates = []
        for day in course_days:
            course_dates.extend(all_dates[all_dates.weekday == day - 1])
        return sorted(course_dates)
    
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
            print(f"Data transformed and loaded into {table_name} successfuly.")

        except(Exception, Error) as error:
            print(f"Error while inserting transformed data into {table_name}: {error}")
            self.connection.rollback()
        
    def run(self):
        warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")
        ddl_file_path = "wh_ddl.sql"
        self.execute_ddl_from_file(ddl_file_path)
        self.transform_and_load()
        self.close_db()
        print("Transformation process is finished.")
        print("------------------------------------------")