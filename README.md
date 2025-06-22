# GoMyCode DataOps Internship Task

## 📦 Project Overview

This project simulates a real-world DataOps task: building an ETL pipeline, modeling both relational and graph data, and delivering visual insights through Looker Studio. It is divided into two main tasks:

- **Task 1**: OLTP to OLAP transformation and visualization
- **Task 2**: Graph data modeling using Neo4j + SQL conversion

---

## 🧪 Task 1 – ETL Pipeline + OLAP + Dashboard

### ✅ Goal

Transform transactional student subscription data from CSV into an analytical OLAP structure that supports:

- Subscription trend analysis
- Churn/completion tracking
- Cohort and demographic analysis

### 🔧 Technologies Used

- Python (ETL)
- PostgreSQL (OLAP schema)
- Google Sheets (bridge to Looker)
- Looker Studio (Dashboard)
- SQLAlchemy, Pandas
- Google Sheets API (`gspread`)

---

### 🗃️ Data Pipeline Flow

1. **Extract**:
   - Loaded `student_subscriptions.csv` using Pandas.
2. **Transform**:
   - Cleaned missing values (median/mode for numeric/categorical).
   - Converted progress % to 0–1 scale.
   - Built 4 dimension tables + 1 fact table:
     - `dim_student`, `dim_instructor`, `dim_course_offering`, `dim_time`, `fact_subscription`
   - Linked all foreign keys with generated surrogate IDs.
3. **Load**:
   - Loaded data into PostgreSQL (schema: `public`)
   - Exported to Google Sheets using `gspread` for Looker Studio access

---

### 📊 Dashboard Highlights

<img src="C:\GoMyCode\Capture d'écran 2025-06-22 130847.png" width="800"/>

🔍 Includes:
- Total subscriptions and average progress KPIs
- Subscription progress distribution (bar chart)
- Time series of diploma vs. no diploma
- Student demographics (gender)
- Geo distribution via map (Country)

✅ Filters by:
- Industry
- Country
- Diploma status
- Date range





🧪 Task 2 – Graph Data Modeling (Neo4j)
🎯 Goal
Design and query a graph-based model for student session attendance using Neo4j. The graph represents relationships between students, sessions, instructors, and modules. It enables more intuitive queries around attendance, teaching assignments, and session categorization.

🗺️ Graph Schema Design
Node Types:

(:Student {student_id})

(:Session {session_id})

(:Instructor {instructor})

(:Module {module})

Relationships:

(:Student)-[:ATTENDED]->(:Session)

(:Session)-[:TAUGHT_BY]->(:Instructor)

(:Session)-[:BELONGS_TO]->(:Module)

These were generated from:

students.csv

sessions.csv

attendance.csv

🔁 Cypher Implementation
1. Create ATTENDED Relationship

cypher
Copier
Modifier
LOAD CSV WITH HEADERS FROM 'file:///attendance.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.student_id)) IS NULL AND NOT toInteger(trim(row.session_id)) IS NULL
MATCH (st:Student { student_id: toInteger(trim(row.student_id)) })
MATCH (se:Session { session_id: toInteger(trim(row.session_id)) })
MERGE (st)-[:ATTENDED]->(se);
2. Create TAUGHT_BY Relationship

c
Copier
Modifier
LOAD CSV WITH HEADERS FROM 'file:///sessions.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.session_id)) IS NULL AND NOT row.instructor IS NULL
MATCH (s:Session { session_id: toInteger(trim(row.session_id)) })
MATCH (i:Instructor { instructor: row.instructor })
MERGE (s)-[:TAUGHT_BY]->(i);
3. Create BELONGS_TO Relationship

cypher
Copier
Modifier
LOAD CSV WITH HEADERS FROM 'file:///sessions.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.session_id)) IS NULL AND NOT row.module IS NULL
MATCH (s:Session { session_id: toInteger(trim(row.session_id)) })
MATCH (m:Module { module: row.module })
MERGE (s)-[:BELONGS_TO]->(m);
❓ Cypher Query Answers
Q1. List all sessions attended by "Alice"

cypher
Copier
Modifier
MATCH (s:Student {name: 'Alice'})-[:ATTENDED]->(sess:Session)
RETURN sess.session_id, sess.date
Q2. Top 3 students with the highest number of attended sessions

cypher
Copier
Modifier
MATCH (s:Student)-[:ATTENDED]->(sess:Session)
RETURN s.name, COUNT(sess) AS session_count
ORDER BY session_count DESC
LIMIT 3
Q3. Count how many students attended each session

cypher
Copier
Modifier
MATCH (s:Session)<-[:ATTENDED]-(st:Student)
RETURN s.session_id, COUNT(st) AS student_count
Q4. For each module, count unique students who attended at least one session

cypher
Copier
Modifier
MATCH (s:Session)-[:BELONGS_TO]->(m:Module)
MATCH (s)<-[:ATTENDED]-(st:Student)
RETURN m.module, COUNT(DISTINCT st) AS unique_students
Q5. Students who never attended any session

cypher
Copier
Modifier
MATCH (s:Student)
WHERE NOT (s)-[:ATTENDED]->()
RETURN s.name, s.student_id
