CREATE SCHEMA IF NOT EXISTS university_db;

CREATE TABLE IF NOT EXISTS wh__courses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS wh__schedules (
    id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    lecturer_id INTEGER NOT NULL,
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    course_day INTEGER NOT NULL,
    schedule_date DATE NOT NULL,
    week_number INTEGER NOT NULL
    --course_days VARCHAR(255) NOT NULL
    --FOREIGN KEY (course_id) REFERENCES course (id)
);

CREATE TABLE IF NOT EXISTS wh__enrollments (
    id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL,
    schedule_id INTEGER NOT NULL,
    academic_year VARCHAR(9) NOT NULL,
    semester INTEGER NOT NULL CHECK (semester IN (1, 2)),
    enroll_dt DATE NOT NULL
    --FOREIGN KEY (schedule_id) REFERENCES schedule (id)
);

CREATE TABLE IF NOT EXISTS wh__attendances (
    id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL,
    schedule_id INTEGER NOT NULL,
    attend_dt DATE NOT NULL
    --FOREIGN KEY (schedule_id) REFERENCES schedule (id)
);