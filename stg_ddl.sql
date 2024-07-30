CREATE SCHEMA IF NOT EXISTS university_db;

CREATE TABLE IF NOT EXISTS stg__courses (
    id INTEGER,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stg__schedules (
    id INTEGER,
    course_id INTEGER,
    lecturer_id INTEGER,
    start_dt VARCHAR(255),
    end_dt VARCHAR(255),
    course_days VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stg__enrollments (
    id INTEGER,
    student_id INTEGER,
    schedule_id INTEGER,
    academic_year VARCHAR(255),
    semester INTEGER,
    enroll_dt VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stg__attendances (
    id INTEGER,
    schedule_id INTEGER,
    student_id INTEGER,
    attend_dt VARCHAR(255)
);