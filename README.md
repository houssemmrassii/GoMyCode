# 🚀 GoMyCode DataOps Internship Task

## 📦 Project Overview

This project simulates a real-world DataOps challenge: transforming raw CSV data into structured analytical models, visualizing insights through dashboards, and applying graph modeling techniques.

### 🔍 Tasks:
- **Task 1**: ETL pipeline + OLAP modeling + Dashboard
- **Task 2**: Graph modeling using Neo4j + Cypher + SQL translation

---

## 🧪 Task 1 – ETL Pipeline + OLAP + Dashboard

### 🎯 Goal

Convert raw subscription data into an OLAP schema to support:
- Subscription trend analysis
- Student diploma completion (churn/success)
- Cohort and demographic insights

---

### 🔧 Technologies Used
- **Python** (ETL pipeline)
- **PostgreSQL** (OLAP database)
- **Google Sheets** (Looker data source)
- **Looker Studio** (Dashboard)
- **SQLAlchemy**, **Pandas**, **gspread**

---

### 🗃️ ETL Pipeline Flow

1. **Extract**:
   - Loaded `student_subscriptions.csv` using Pandas.

2. **Transform**:
   - Cleaned missing values using median/mode.
   - Normalized progress values (`0–1` scale).
   - Built and loaded 4 dimensions + 1 fact table:
     - `dim_student`, `dim_instructor`, `dim_course_offering`, `dim_time`, `fact_subscription`

3. **Load**:
   - Loaded data into **PostgreSQL** (`public` schema)
   - Exported OLAP tables to **Google Sheets** via `gspread` for Looker Studio

---

### 📊 Dashboard Highlights



<img src="C:\GoMyCode\Capture d'écran 2025-06-22 130847.png" width="800"/>

#### Includes:
- KPI Cards (Total Subscriptions, Avg Progress)
- Diploma Completion Rate (Pie/Stacked Bar)
- Subscription Trends Over Time (Line)
- Progress by Country (Bar)
- Gender Analysis (Bar)
- Filters: Country, Industry, Diploma, Date Range

---

## 🧪 Task 2 – Graph Data Modeling (Neo4j)

### 🎯 Goal

Use Neo4j to model and query relationships between:
- Students
- Sessions
- Instructors
- Modules

This graph approach makes it easy to analyze participation, teaching relationships, and course coverage.

---

### 🗺️ Graph Schema Design

**Node Types**:
- `(:Student {student_id})`
- `(:Session {session_id})`
- `(:Instructor {instructor})`
- `(:Module {module})`

**Relationships**:
- `(:Student)-[:ATTENDED]->(:Session)`
- `(:Session)-[:TAUGHT_BY]->(:Instructor)`
- `(:Session)-[:BELONGS_TO]->(:Module)`

**Source Files**:
- `students.csv`
- `sessions.csv`
- `attendance.csv`

---

### 🔁 Cypher Implementation

```cypher
// ATTENDED relationship
LOAD CSV WITH HEADERS FROM 'file:///attendance.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.student_id)) IS NULL AND NOT toInteger(trim(row.session_id)) IS NULL
MATCH (st:Student { student_id: toInteger(trim(row.student_id)) })
MATCH (se:Session { session_id: toInteger(trim(row.session_id)) })
MERGE (st)-[:ATTENDED]->(se);

// TAUGHT_BY relationship
LOAD CSV WITH HEADERS FROM 'file:///sessions.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.session_id)) IS NULL AND NOT row.instructor IS NULL
MATCH (s:Session { session_id: toInteger(trim(row.session_id)) })
MATCH (i:Instructor { instructor: row.instructor })
MERGE (s)-[:TAUGHT_BY]->(i);

// BELONGS_TO relationship
LOAD CSV WITH HEADERS FROM 'file:///sessions.csv' AS row
WITH row
WHERE NOT toInteger(trim(row.session_id)) IS NULL AND NOT row.module IS NULL
MATCH (s:Session { session_id: toInteger(trim(row.session_id)) })
MATCH (m:Module { module: row.module })
MERGE (s)-[:BELONGS_TO]->(m);
