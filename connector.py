#!/usr/bin/env python3
import json
import datetime
import base64
import struct
from kafka import KafkaConsumer
import clickhouse_connect

# اتصال به ClickHouse
client = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='default',
    password='',
    database='default'
)

# تبدیل Base64 NUMERIC → float
def decode_decimal(val):
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except:
            try:
                decoded = base64.b64decode(val)
                if len(decoded) == 8:
                    return struct.unpack('>d', decoded)[0]
            except:
                pass
    return 0.0

# تبدیل تاریخ (روز → DateTime)
def to_datetime_from_days(days):
    if days is None:
        return datetime.datetime.now()
    if isinstance(days, int):
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=days)
    return datetime.datetime.now()

# تبدیل timestamp
def to_datetime(ts):
    if ts is None:
        return datetime.datetime.now()
    if isinstance(ts, (int, float)):
        if ts > 1e18: ts /= 1e6
        elif ts > 1e15: ts /= 1e3
        elif ts > 1e12: ts /= 1000
        try:
            return datetime.datetime.fromtimestamp(ts)
        except:
            return datetime.datetime.now()
    return datetime.datetime.now()

# تبدیل عدد — هیچوقت None نباشه
def to_int(val, default=0):
    if val is None:
        return default
    try:
        return int(val)
    except:
        return default

# مپینگ دقیق تاپیک → جدول + ستون‌های واقعی (از SHOW CREATE TABLE که فرستادی)
TABLE_COLUMNS = {
    "students": ["id", "name", "email", "created_at", "updated_at", "op", "ts_ms"],
    "instructors": ["id", "name", "bio", "created_at", "updated_at", "op", "ts_ms"],
    "courses": ["id", "instructor_id", "title", "description", "price", "created_at", "updated_at", "op", "ts_ms"],
    "enrollments": ["id", "student_id", "course_id", "enrolled_at", "op", "ts_ms"],
    "lessons": ["id", "course_id", "title", "content", "created_at", "updated_at", "op", "ts_ms"],
    "logs": ["id", "student_id", "course_id", "action", "logged_at", "op", "ts_ms"],
    "payments": ["id", "student_id", "course_id", "amount", "payment_date", "op", "ts_ms"],
    "quizzes": ["id", "course_id", "question", "answer", "created_at", "updated_at", "op", "ts_ms"],
    "reviews": ["id", "student_id", "course_id", "rating", "comment", "created_at", "updated_at", "op", "ts_ms"],
    "certificates": ["id", "student_id", "course_id", "issued_at", "op", "ts_ms"],
}

TOPIC_TO_TABLE = {
    "online_course.public.students": "students",
    "online_course.public.instructors": "instructors",
    "online_course.public.courses": "courses",
    "online_course.public.lessons": "lessons",
    "online_course.public.enrollments": "enrollments",
    "online_course.public.payments": "payments",
    "online_course.public.reviews": "reviews",
    "online_course.public.quizzes": "quizzes",
    "online_course.public.certificates": "certificates",
    "online_course.public.logs": "logs",
}

batch = {table: [] for table in TABLE_COLUMNS.keys()}

def flush():
    for table, rows in batch.items():
        if not rows:
            continue
        columns = TABLE_COLUMNS[table]
        data = []
        for row in rows:
            data.append([row.get(col, None) for col in columns])
        try:
            client.insert(table, data, column_names=columns)
            print(f"Inserted {len(rows)} rows into {table}")
        except Exception as e:
            print(f"Insert failed for {table}: {e}")
        batch[table].clear()

consumer = KafkaConsumer(
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    group_id='ch_sink_final_ok',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    max_poll_records=1000
)

consumer.subscribe(list(TOPIC_TO_TABLE.keys()))
print("Starting sync... (Ctrl+C to stop)")

total = 0
now = datetime.datetime.now()

for message in consumer:
    payload = message.value.get("payload", {})
    if not payload:
        continue

    topic = message.topic
    table = TOPIC_TO_TABLE.get(topic)
    if not table:
        continue

    op = payload.get("op", "u")
    ts_ms = payload.get("ts_ms", 0)
    after = payload.get("after")
    before = payload.get("before")

    row = {}

    if op == "d" and before:
        row = {col: None for col in TABLE_COLUMNS[table]}
        row["id"] = to_int(before.get("id"))
        row["op"] = "d"
        row["ts_ms"] = ts_ms
    elif after:
        row = {col: None for col in TABLE_COLUMNS[table]}

        if table == "students":
            row.update({
                "id": to_int(after.get("id")),
                "name": after.get("full_name") or "",
                "email": after.get("email") or "",
                "created_at": to_datetime_from_days(after.get("registration_date")),
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "instructors":
            row.update({
                "id": to_int(after.get("id")),
                "name": after.get("full_name") or "",
                "bio": after.get("bio") or "",
                "created_at": to_datetime_from_days(after.get("hire_date")),
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "courses":
            row.update({
                "id": to_int(after.get("id")),
                "instructor_id": to_int(after.get("instructor_id")),
                "title": after.get("title") or "",
                "description": after.get("description") or "",
                "price": decode_decimal(after.get("price")),
                "created_at": to_datetime_from_days(after.get("created_at")),
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "lessons":
            row.update({
                "id": to_int(after.get("id")),
                "course_id": to_int(after.get("course_id")),
                "title": after.get("title") or "",
                "content": after.get("content_url") or "",
                "created_at": now,
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "enrollments":
            row.update({
                "id": to_int(after.get("id")),
                "student_id": to_int(after.get("student_id")),
                "course_id": to_int(after.get("course_id")),
                "enrolled_at": to_datetime(after.get("enrolled_at")),
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "payments":
            row.update({
                "id": to_int(after.get("id")),
                "student_id": to_int(after.get("student_id")),
                "course_id": to_int(after.get("course_id")),
                "amount": decode_decimal(after.get("amount")),
                "payment_date": to_datetime(after.get("paid_at")),
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "reviews":
            row.update({
                "id": to_int(after.get("id")),
                "student_id": to_int(after.get("student_id")),
                "course_id": to_int(after.get("course_id")),
                "rating": to_int(after.get("rating"), 0),
                "comment": after.get("comment") or "",
                "created_at": to_datetime(after.get("created_at")),
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "quizzes":
            row.update({
                "id": to_int(after.get("id")),
                "course_id": to_int(after.get("course_id")),
                "question": after.get("question") or "",
                "answer": after.get("answer") or "",
                "created_at": now,
                "updated_at": now,
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "certificates":
            row.update({
                "id": to_int(after.get("id")),
                "student_id": to_int(after.get("student_id")),
                "course_id": to_int(after.get("course_id")),
                "issued_at": to_datetime(after.get("issued_at")),
                "op": op,
                "ts_ms": ts_ms
            })

        elif table == "logs":
            row.update({
                "id": to_int(after.get("id")),
                "student_id": to_int(after.get("student_id")),
                "course_id": to_int(after.get("course_id")),
                "action": after.get("action") or "",
                "logged_at": to_datetime(after.get("created_at")),
                "op": op,
                "ts_ms": ts_ms
            })

    else:
        continue

    batch[table].append(row)
    total += 1

    if total % 5000 == 0:
        flush()
        print(f"Processed {total} messages...")

flush()
print(f"Finished! Total processed: {total}")