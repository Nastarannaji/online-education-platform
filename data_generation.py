# data_generate.py
import json
import random
from datetime import datetime, timedelta

import psycopg2
from faker import Faker

# Install required packages if not already installed
try:
    import pandas
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "Faker", "pandas"])
    import pandas

fake = Faker()
Faker.seed(0)
random.seed(0)

# -----------------------------
# PostgreSQL connection settings
# -----------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "online_course",
    "user": "admin",
    "password": "admin",
}

# -----------------------------
# Number of records for each table
# -----------------------------
NUM_STUDENTS = 200
NUM_INSTRUCTORS = 20
NUM_COURSES = 40
NUM_LESSONS_PER_COURSE = (5, 15)      # random range
NUM_ENROLLMENTS_PER_STUDENT = (1, 6)
NUM_REVIEWS_PER_ENROLLMENT = (0, 1)   # either zero or one
NUM_LOGS = 2000


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# -----------------------------
# Data generation functions
# -----------------------------
def create_students(cur):
    students = []
    for _ in range(NUM_STUDENTS):
        name = fake.name()
        age = random.randint(18, 60)
        email = fake.unique.email()
        city = fake.city()
        reg_date = fake.date_between(start_date="-2y", end_date="today")
        students.append((name, age, email, city, reg_date))

    cur.executemany(
        """
        INSERT INTO students (full_name, age, email, city, registration_date)
        VALUES (%s, %s, %s, %s, %s);
        """,
        students,
    )
    print(f"Inserted {NUM_STUDENTS} students")


def create_instructors(cur):
    instructors = []
    for _ in range(NUM_INSTRUCTORS):
        name = fake.name()
        email = fake.unique.email()
        city = fake.city()
        bio = fake.text()[:200]
        hire_date = fake.date_between(start_date="-3y", end_date="-1y")
        instructors.append((name, email, city, bio, hire_date))

    cur.executemany(
        """
        INSERT INTO instructors (full_name, email, city, bio, hire_date)
        VALUES (%s, %s, %s, %s, %s);
        """,
        instructors,
    )
    print(f"Inserted {NUM_INSTRUCTORS} instructors")


def create_courses(cur):
    # Get actual instructor IDs from the database
    cur.execute("SELECT id FROM instructors;")
    instructor_ids = [row[0] for row in cur.fetchall()]

    levels = ["beginner", "intermediate", "advanced"]
    courses = []

    for _ in range(NUM_COURSES):
        title = f"{fake.word().title()} Course"
        description = fake.text()[:300]
        level = random.choice(levels)
        price = random.choice([0, 19.0, 29.0, 49.0, 99.0])

        # Instructor ID must be selected from REAL IDs
        instructor_id = random.choice(instructor_ids)

        created_at = fake.date_between(start_date="-2y", end_date="-1m")

        courses.append((title, description, level, price, instructor_id, created_at))

    cur.executemany(
        """
        INSERT INTO courses (title, description, level, price, instructor_id, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        courses,
    )
    print(f"Inserted {NUM_COURSES} courses")


def create_lessons(cur):
    # Get real course IDs
    cur.execute("SELECT id FROM courses;")
    course_ids = [row[0] for row in cur.fetchall()]

    lessons = []

    for course_id in course_ids:
        num_lessons = random.randint(*NUM_LESSONS_PER_COURSE)
        for i in range(num_lessons):
            title = f"Lesson {i+1} for course {course_id}"
            content_url = fake.url()
            order_index = i + 1
            duration = random.randint(5, 30)  # minutes
            lessons.append((course_id, title, content_url, order_index, duration))

    cur.executemany(
        """
        INSERT INTO lessons (course_id, title, content_url, order_index, duration_minutes)
        VALUES (%s, %s, %s, %s, %s);
        """,
        lessons,
    )
    print(f"Inserted {len(lessons)} lessons")



def create_enrollments_and_payments_and_reviews(cur):
    # Fetch real student IDs and course IDs
    cur.execute("SELECT id FROM students;")
    student_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT id FROM courses;")
    course_ids_all = [row[0] for row in cur.fetchall()]

    enrollment_rows = []

    for student_id in student_ids:
        num_enrollments = random.randint(*NUM_ENROLLMENTS_PER_STUDENT)
        course_ids = random.sample(course_ids_all, num_enrollments)
        for course_id in course_ids:
            enrolled_at = fake.date_time_between(start_date="-1y", end_date="now")
            status = random.choice(["active", "completed", "cancelled"])
            enrollment_rows.append((student_id, course_id, enrolled_at, status))

    # Insert enrollments first
    cur.executemany(
        """
        INSERT INTO enrollments (student_id, course_id, enrolled_at, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """,
        enrollment_rows,
    )
    # Fetch actual enrollment IDs
    cur.execute("SELECT id FROM enrollments;")
    enrollment_ids = [row[0] for row in cur.fetchall()]

    # Create payments and reviews based on real enrollment IDs
    payment_rows = []
    review_rows = []

    for enrollment_id in enrollment_ids:
        # Payment
        payment_status = random.choice(["paid", "failed", "refunded"])
        paid_at = fake.date_time_between(start_date="-1y", end_date="now")
        method = random.choice(["credit_card", "paypal", "stripe"])
        payment_rows.append((enrollment_id, payment_status, paid_at, method))

        # Review (60% probability)
        if random.randint(0, 100) < 60:
            rating = random.randint(1, 5)
            comment = fake.sentence(nb_words=15)
            created_at = fake.date_time_between(start_date="-1y", end_date="now")
            review_rows.append((enrollment_id, rating, comment, created_at))

    # Insert payments
    cur.executemany(
        """
        INSERT INTO payments (enrollment_id, status, paid_at, method)
        VALUES (%s, %s, %s, %s);
        """,
        payment_rows,
    )
    print(f"Inserted {len(payment_rows)} payments")

    # Insert reviews
    cur.executemany(
        """
        INSERT INTO reviews (enrollment_id, rating, comment, created_at)
        VALUES (%s, %s, %s, %s);
        """,
        review_rows,
    )
    print(f"Inserted {len(review_rows)} reviews")



def create_quizzes(cur):
    # Get real lesson IDs from DB
    cur.execute("SELECT id FROM lessons;")
    lesson_ids = [row[0] for row in cur.fetchall()]

    quizzes = []

    for lesson_id in lesson_ids:
        if random.random() < 0.7:  # not all lessons have quizzes
            title = f"Quiz for lesson {lesson_id}"
            num_questions = random.randint(5, 20)
            passing_score = random.randint(50, 80)
            quizzes.append((lesson_id, title, num_questions, passing_score))

    cur.executemany(
        """
        INSERT INTO quizzes (lesson_id, title, num_questions, passing_score)
        VALUES (%s, %s, %s, %s);
        """,
        quizzes,
    )
    print(f"Inserted {len(quizzes)} quizzes")


def create_certificates(cur):
    certificates = []
    # Only for enrollments with 'completed' status
    cur.execute(
        """
        SELECT id, enrolled_at
        FROM enrollments
        WHERE status = 'completed';
        """
    )
    for enrollment_id, enrolled_at in cur.fetchall():
        issued_at = enrolled_at + timedelta(days=random.randint(10, 120))
        code = f"CERT-{enrollment_id:06d}-{random.randint(1000, 9999)}"
        certificates.append((enrollment_id, issued_at, code))

    if certificates:
        cur.executemany(
            """
            INSERT INTO certificates (enrollment_id, issued_at, certificate_code)
            VALUES (%s, %s, %s);
            """,
            certificates,
        )
    print(f"Inserted {len(certificates)} certificates")


def create_logs(cur):
    # Fetch real student IDs and course IDs
    cur.execute("SELECT id FROM students;")
    student_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT id FROM courses;")
    course_ids = [row[0] for row in cur.fetchall()]

    actions = [
        "login",
        "logout",
        "watch_lesson",
        "start_quiz",
        "finish_quiz",
        "download_certificate",
    ]

    logs = []

    for _ in range(NUM_LOGS):
        student_id = random.choice(student_ids)
        course_id = random.choice(course_ids)
        action = random.choice(actions)
        created_at = fake.date_time_between(start_date="-1y", end_date="now")
        metadata = {"ip": fake.ipv4(), "user_agent": fake.user_agent()}
        logs.append((student_id, course_id, action, created_at, json.dumps(metadata)))

    cur.executemany(
        """
        INSERT INTO logs (student_id, course_id, action, created_at, metadata)
        VALUES (%s, %s, %s, %s, %s);
        """,
        logs,
    )
    print(f"Inserted {len(logs)} logs")



# -----------------------------
# Execute all steps
# -----------------------------
def main():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                create_students(cur)
                create_instructors(cur)
                create_courses(cur)
                create_lessons(cur)
                create_enrollments_and_payments_and_reviews(cur)
                create_quizzes(cur)
                create_certificates(cur)
                create_logs(cur)
        print("DONE.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()