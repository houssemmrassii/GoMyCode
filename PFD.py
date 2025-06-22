import pandas as pd
from sqlalchemy import create_engine

# Update with your database credentials
engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5433/ETL')

# Replace with the table you want to export
tables = ['fact_subscription', 'dim_student', 'dim_instructor', 'dim_course_offering', 'dim_time']

for table in tables:
    df = pd.read_sql_table(table, con=engine, schema='public')
    df.to_csv(f'{table}.csv', index=False)
    print(f"✅ Exported {table} to {table}.csv")
