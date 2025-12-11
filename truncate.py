import clickhouse_connect

# اتصال به ClickHouse
ch = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password=""
)

# لیست تمام جدول‌ها
tables = [
    "students",
    "courses",
    "instructors",
    "enrollments",
    "lessons",
    "logs",
    "payments",
    "quizzes",
    "reviews",
    "certificates"
]

# خالی کردن همه جدول‌ها
for table in tables:
    try:
        ch.command(f"TRUNCATE TABLE {table}")
        print(f"✅ جدول {table} خالی شد")
    except Exception as e:
        print(f"⚠️ خطا در خالی کردن جدول {table}: {e}")
