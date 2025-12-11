from datetime import datetime, timedelta
import time
import random, string
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------- Helper Functions ----------------
def random_string(prefix="", length=5):
    return prefix + ''.join(random.choices(string.ascii_lowercase, k=length))

def insert_full_student_flow():
    hook = PostgresHook(postgres_conn_id="postgresS")
    conn = hook.get_conn()
    cur = conn.cursor()

    # ---------------- Add Instructor ----------------
    instructor_name = random_string("Instructor ")
    instructor_email = instructor_name.replace(" ", "_") + "@example.com"
    city = random.choice(["Tehran", "Isfahan", "Shiraz"])
    bio = f"Bio of {instructor_name}"

    cur.execute("""
        INSERT INTO instructors (full_name, email, city, bio, hire_date)
        VALUES (%s, %s, %s, %s, CURRENT_DATE) RETURNING id
    """, (instructor_name, instructor_email, city, bio))
    instructor_id = cur.fetchone()[0]

    # ---------------- Add Course ----------------
    course_title = random_string("Course ", 8)
    course_description = f"Description for {course_title}"
    course_level = random.choice(["Beginner", "Intermediate", "Advanced"])
    course_price = round(random.uniform(10, 500), 2)

    cur.execute("""
        INSERT INTO courses (title, description, level, price, instructor_id, created_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_DATE) RETURNING id
    """, (course_title, course_description, course_level, course_price, instructor_id))
    course_id = cur.fetchone()[0]

    # ---------------- Add Lessons ----------------
    lessons = []
    for i in range(1, random.randint(3, 6)):
        lesson_title = f"Lesson {i} for {course_title}"
        content_url = f"https://example.com/{lesson_title.replace(' ', '_')}"
        duration = random.randint(5, 60)
        cur.execute("""
            INSERT INTO lessons (course_id, title, content_url, order_index, duration_minutes)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (course_id, lesson_title, content_url, i, duration))
        lessons.append(cur.fetchone()[0])

    # ---------------- Add Student ----------------
    student_name = random_string("Student ")
    student_email = student_name.replace(" ", "_") + "@example.com"
    age = random.randint(18, 60)
    student_city = "Tehran"

    cur.execute("""
        INSERT INTO students (full_name, age, email, city, registration_date)
        VALUES (%s, %s, %s, %s, CURRENT_DATE) RETURNING id
    """, (student_name, age, student_email, student_city))
    student_id = cur.fetchone()[0]

    # ---------------- Enroll Student ----------------
    cur.execute("""
        INSERT INTO enrollments (student_id, course_id, enrolled_at, status)
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s) RETURNING id
    """, (student_id, course_id, "active"))
    enrollment_id = cur.fetchone()[0]

    # ---------------- Payment ----------------
    cur.execute("""
        INSERT INTO payments (enrollment_id, status, paid_at, method)
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
    """, (enrollment_id, "paid", random.choice(["credit_card", "paypal", "bank_transfer"])))

    # ---------------- Reviews ----------------
    if random.choice([True, False]):
        rating = random.randint(1, 5)
        comment = f"This is a review for {course_title}"
        cur.execute("""
            INSERT INTO reviews (enrollment_id, rating, comment, created_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """, (enrollment_id, rating, comment))

    # ---------------- Quizzes ----------------
    for lesson_id in lessons:
        if random.choice([True, False]):
            quiz_title = f"Quiz for lesson {lesson_id}"
            num_questions = random.randint(5, 15)
            passing_score = random.randint(50, 100)
            cur.execute("""
                INSERT INTO quizzes (lesson_id, title, num_questions, passing_score)
                VALUES (%s, %s, %s, %s)
            """, (lesson_id, quiz_title, num_questions, passing_score))

    # ---------------- Certificates ----------------
    if random.choice([True, False]):
        certificate_code = f"CERT-{student_id}-{course_id}-{random.randint(1000,9999)}"
        cur.execute("""
            INSERT INTO certificates (enrollment_id, issued_at, certificate_code)
            VALUES (%s, CURRENT_TIMESTAMP, %s)
        """, (enrollment_id, certificate_code))

    # ---------------- Logs ----------------
    for lesson_id in lessons:
        cur.execute("""
            INSERT INTO logs (student_id, course_id, action, created_at, metadata)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
        """, (student_id, course_id, "viewed_lesson", f'{{"lesson_id": {lesson_id}}}'))

    # ---------------- Commit and Close ----------------
    conn.commit()
    cur.close()
    conn.close()

def delayed_insert():
    time.sleep(30)  # 30 second delay
    insert_full_student_flow()

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    "dag2",
    start_date=datetime(2025,1,1),
    schedule_interval="*/1 * * * *",  # every 1 minute
    catchup=False,
    default_args=default_args
) as dag2:
    t2 = PythonOperator(
        task_id="insert_student_dag2",
        python_callable=delayed_insert
    )
