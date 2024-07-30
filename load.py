import pandas as pd
import psycopg2
from psycopg2 import Error
from config import Config

class Loader:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect_to_db()
        print("Starting load process...")

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

    def fetch_data(self):
        """Fetch data from the datawarehouse tables"""
        try:
            query = """
            WITH schedule_sum AS (
                SELECT
                    course_id,
                    schedule_date,
                    week_number,
                    CASE
                        WHEN schedule_date < '2019-12-31' THEN 1
                        WHEN schedule_date > '2020-01-01' THEN 2
                    END AS semester
                FROM wh__schedules
            ),
            attendance_sum AS (
                SELECT
                    schedule_id,
                    attend_dt,
                    COUNT(student_id) AS student_atd
                FROM wh__attendances
                GROUP BY schedule_id, attend_dt
            ),
            enrollment_num AS (
                SELECT
                    schedule_id,
                    COUNT(student_id) AS student_enr
                FROM wh__enrollments
                GROUP BY schedule_id
            ),
            attendance_pct AS (
                SELECT
                    s.course_id,
                    s.schedule_date,
                    s.week_number,
                    s.semester,
                    COALESCE(a.student_atd, 0) AS student_attend,
                    COALESCE(e.student_enr, 0) AS student_enrolled
                FROM schedule_sum AS s
                LEFT JOIN attendance_sum AS a
                    ON s.course_id = a.schedule_id
                    AND s.schedule_date = a.attend_dt
                LEFT JOIN enrollment_num AS e
                    ON s.course_id = e.schedule_id
            )
            SELECT
                c.name AS course_name,
                p.semester,
                p.week_number,
                ROUND((SUM(p.student_attend) / NULLIF(SUM(p.student_enrolled), 0) * 100), 2) AS attendance_percentage
            FROM attendance_pct AS p
            LEFT JOIN wh__courses AS c
                ON p.course_id = c.id
            GROUP BY p.semester, c.name, p.course_id, p.week_number
            HAVING SUM(p.student_enrolled) > 0
            ORDER BY p.semester, p.course_id, p.week_number 
            """
            
            df = pd.read_sql_query(query, self.connection)
            return df
        
        except(Exception, Error) as error:
            print(f"Error while fetching data: {error}")
            return None
        
    def create_table(self):
        """Create the data mart table for reporting"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS mart__weekly_attendance (
            course_name VARCHAR(255),
            semester INT,
            week_number INT,
            attendance_percentage NUMERIC(5, 2)
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
            print("Table 'mart__weekly_attendance' created successfully.")
        except(Exception, Error) as error:
            print(f"Error while creating table: {error}")
            self.connection.rollback()
    
    def ingest_data(self, df):
        """Ingest data to the mart table"""
        if df is None or df.empty:
            print("No data to ingest.")
            return
        
        try:
            for row in df.itertuples(index=False):
                insert_query = """
                INSERT INTO mart__weekly_attendance (course_name, semester, week_number, attendance_percentage)
                VALUES (%s, %s, %s, %s);
                """
                self.cursor.execute(insert_query, (row.course_name, row.semester, row.week_number, row.attendance_percentage))
            self.connection.commit()
            print("Data inserted into 'mart__weekly_attendance' successfully.")

        except(Exception, Error) as error:
            print(f"Error while inserting data: {error}")
            self.connection.rollback()

    def generate_csv_report(self, df):
        """Generate csv report from the mart data"""
        if df is None or df.empty:
            print("No data to generate report")
            return
        
        try:
            df.to_csv('weekly_attendance_report.csv', index=False)
            print("CSV report generated successfully")
        except(Exception, Error) as error:
            print(f"Error while generating CSV report: {error}")
        
    def run(self):
        """Run the load proses"""
        self.create_table()
        data = self.fetch_data()
        self.ingest_data(data)
        self.generate_csv_report(data)
        self.close_db()
        print("Load process is finished.")
        print("------------------------------------------")