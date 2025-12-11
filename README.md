# online-education-platform

Online Education Platform Data Pipeline
Project Overview

This project implements a complete end-to-end data pipeline for an online education platform. The pipeline captures, processes, and analyzes operational data in real-time, allowing insights through a visualization layer.

The pipeline integrates the following technologies:

PostgreSQL – relational database for storing system data.

Python – generates realistic fake data.

Apache Airflow – schedules automated data generation.

Debezium + Kafka – captures database changes in real-time (CDC) and streams them.

ClickHouse – analytical database optimized for fast queries.

Apache Superset – visualization and dashboarding layer.

Docker – containerizes all services for easy deployment.

Architecture

The pipeline follows this structure:

    Python Data Generation → PostgreSQL → Debezium (CDC) → Kafka → ClickHouse → Apache Superset
                           ↑
                       Apache Airflow (Scheduler)


Database Schema
Tables and Key Fields
Table	Key Columns & Relationships
 ```
students	id (PK), full_name, age, email (unique), city, registration_date
instructors	id (PK), full_name, email (unique), city, bio, hire_date
courses	id (PK), title, description, level, price, instructor_id (FK → instructors.id), created_at
lessons	id (PK), course_id (FK → courses.id), title, content_url, order_index, duration_minutes
enrollments	id (PK), student_id (FK → students.id), course_id (FK → courses.id), enrolled_at, status
payments	id (PK), enrollment_id (FK → enrollments.id), status, paid_at, method
reviews	id (PK), enrollment_id (FK → enrollments.id), rating, comment, created_at
quizzes	id (PK), lesson_id (FK → lessons.id), title, num_questions, passing_score
certificates	id (PK), enrollment_id (FK → enrollments.id), issued_at, certificate_code
logs	id (PK), student_id (FK → students.id), course_id (FK → courses.id), action, created_at, metadata (JSONB)
```

All components are containerized with Docker.

Relationships Overview:

A student can enroll in many courses → enrollments table.

A course can have multiple lessons and enrollments.

Payments, reviews, and certificates are linked to enrollments.

Quizzes are linked to lessons.

Logs capture student actions per course with additional metadata.

Installation and Setup (Manual Step Sequence)
Prerequisites

Docker & Docker Compose

Python 3.x  

Step-by-Step Setup

Start PostgreSQL

Bring up the PostgreSQL container:

    docker-compose up -d postgres


Ensure the database is running before generating any data.

Generate Initial Data

Run the Python script to populate PostgreSQL tables with fake data:

    python3 data_generate.py


This ensures tables (students, courses, enrollments, etc.) have data for downstream processing.

Configure Apache Airflow

Start the Airflow container:

    docker-compose up -d airflow


Open Airflow UI (localhost:8080) and create a connection to PostgreSQL so DAGs can access the database.

Trigger the DAGs that insert new students and related records every 30 seconds.

Set Up Kafka & Debezium

Start Kafka and Debezium containers:
  
      docker-compose up -d kafka debezium


Register the PostgreSQL CDC connector by sending the JSON config file:

curl -X POST -H "Content-Type: application/json" --data @pg-connector.json http://localhost:8083/connectors


Verify that Kafka topics receive the changes from PostgreSQL.

Set Up ClickHouse and Load Data

Start ClickHouse container:

      docker-compose up -d clickhouse


Create the analytical tables in ClickHouse (schema must match what you want to analyze).

Run the Python consumer script to read Kafka messages and insert them into ClickHouse:

    python3 connector.py


Verify that ClickHouse has received all the data correctly.

Set Up Apache Superset

Start Superset container:

    docker-compose up -d superset


Connect Superset to ClickHouse via the Superset UI (localhost:9090).

Create datasets and write queries to prepare your dashboards.

Build dashboards to visualize metrics like:

New students per month

Most popular courses

Monthly revenue

Average exam scores
