import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.sql import text

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
SCHEMA = os.getenv('SCHEMA')

# CSV file path
script_dir = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(script_dir, 'Functional Task - OLTP_Subscription.csv')

# Create database engine
def get_engine():
    engine_url = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding=utf8'
    return create_engine(engine_url)

# Drop and recreate schema
def reset_schema(engine):
    with engine.connect() as connection:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;"))
        connection.execute(text(f"CREATE SCHEMA {SCHEMA};"))
        connection.commit()
    print("Schema reset successfully.")

# Define and create tables
def create_tables(engine):
    metadata = MetaData(schema=SCHEMA)
    
    # Dimension Tables
    Table('dim_student', metadata,
          Column('StudentID', Integer, primary_key=True),
          Column('Student', String),
          Column('StudentGender', String),
          Column('StudentBirthDate', DateTime),
          Column('professionalExperience', Integer),
          Column('Industry', String))
    
    Table('dim_instructor', metadata,
          Column('InstructorID', Integer, primary_key=True),
          Column('InstructorFullName', String),
          Column('InstructorEmail', String),
          Column('instructor_diploma', String))
    
    Table('dim_course_offering', metadata,
          Column('CourseOfferingID', Integer, primary_key=True),
          Column('GroupName', String),
          Column('SessionName', String),
          Column('TrackName', String),
          Column('Hackerspace', String),
          Column('Country', String),
          Column('ProductSchedule', String),
          Column('InstructorID', Integer, ForeignKey(f'{SCHEMA}.dim_instructor.InstructorID')))
    
    Table('dim_time', metadata,
          Column('TimeID', Integer, primary_key=True),
          Column('Date', DateTime),
          Column('Year', Integer),
          Column('Month', Integer),
          Column('Day', Integer))
    
    # Fact Table
    Table('fact_subscription', metadata,
          Column('SubscriptionID', Integer, primary_key=True),
          Column('CourseOfferingID', Integer, ForeignKey(f'{SCHEMA}.dim_course_offering.CourseOfferingID')),
          Column('StudentID', Integer, ForeignKey(f'{SCHEMA}.dim_student.StudentID')),
          Column('StartTimeID', Integer, ForeignKey(f'{SCHEMA}.dim_time.TimeID')),
          Column('EndTimeID', Integer, ForeignKey(f'{SCHEMA}.dim_time.TimeID')),
          Column('DiplomaTimeID', Integer, ForeignKey(f'{SCHEMA}.dim_time.TimeID')),
          Column('SubscriptionProgress', Float),
          Column('SubscriptionHasDiploma', Boolean))
    
    metadata.create_all(engine)
    print("Tables created successfully.")

# Process the CSV and load data
def process_csv():
    engine = get_engine()
    reset_schema(engine)
    create_tables(engine)
    
    # Extract
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file {CSV_FILE} not found.")
        return
    
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_FILE, encoding='cp1252')
    
    # Clean data: handle missing values with mean/median/mode
    # Numerical: Use median for professionalExperience
    df['professionalExperience'] = df['professionalExperience'].fillna(df['professionalExperience'].median())
    
    # Categorical: Use mode for StudentGender and Industry
    df['StudentGender'] = df['StudentGender'].fillna(df['StudentGender'].mode()[0])
    df['Industry'] = df['Industry'].fillna(df['Industry'].mode()[0])
    
    # Dates: Convert to datetime and fill with median date
    date_columns = ['StudentBirthDate', 'SubscriptionStartDate', 'SubscriptionEndDate', 'DiplomaDate']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        median_date = df[col].median()
        df[col] = df[col].fillna(median_date)
    
    # Handle SubscriptionProgress and SubscriptionHasDiploma
    df['SubscriptionProgress'] = df['SubscriptionProgress'].fillna('0%').str.rstrip('%').astype(float) / 100.0
    df['SubscriptionHasDiploma'] = df['SubscriptionHasDiploma'].fillna('FALSE').map({'TRUE': True, 'FALSE': False})

    # Transform
    # Dim Instructor
    dim_instructor = df[['InstructorFullName', 'InstructorEmail', 'instructor_diploma']].drop_duplicates().reset_index(drop=True)
    dim_instructor['InstructorID'] = dim_instructor.index + 1
    
    # Dim Course Offering (deduplicate based on business key)
    course_offering_key = ['GroupName', 'SessionName', 'TrackName', 'Hackerspace', 'Country', 'ProductSchedule']
    dim_course_offering = df[course_offering_key + ['InstructorFullName', 'InstructorEmail']].drop_duplicates().reset_index(drop=True)
    dim_course_offering = dim_course_offering.merge(dim_instructor[['InstructorFullName', 'InstructorEmail', 'InstructorID']], 
                                                  on=['InstructorFullName', 'InstructorEmail'], how='left')
    dim_course_offering = dim_course_offering[course_offering_key + ['InstructorID']].reset_index(drop=True)
    dim_course_offering['CourseOfferingID'] = dim_course_offering.index + 1
    
    # Dim Student
    dim_student = df[['Student', 'StudentGender', 'StudentBirthDate', 'professionalExperience', 'Industry']].drop_duplicates().reset_index(drop=True)
    dim_student['StudentID'] = dim_student.index + 1
    
    # Dim Time
    dates = pd.concat([df['SubscriptionStartDate'], df['SubscriptionEndDate'], df['DiplomaDate']]).dropna().unique()
    dim_time = pd.DataFrame({'Date': pd.to_datetime(dates, errors='coerce')})
    dim_time['TimeID'] = dim_time.index + 1
    dim_time['Year'] = dim_time['Date'].dt.year
    dim_time['Month'] = dim_time['Date'].dt.month
    dim_time['Day'] = dim_time['Date'].dt.day
    
    # Fact Subscription
    fact_subscription = df[['GroupName', 'Student', 'SubscriptionStartDate', 'SubscriptionEndDate', 'DiplomaDate', 
                           'SubscriptionProgress', 'SubscriptionHasDiploma']]
    fact_subscription = fact_subscription.merge(dim_course_offering[['GroupName', 'CourseOfferingID']], on='GroupName', how='left')
    fact_subscription = fact_subscription.merge(dim_student[['Student', 'StudentID']], on='Student', how='left')
    
    fact_subscription['SubscriptionStartDate'] = pd.to_datetime(fact_subscription['SubscriptionStartDate'], errors='coerce')
    fact_subscription['SubscriptionEndDate'] = pd.to_datetime(fact_subscription['SubscriptionEndDate'], errors='coerce')
    fact_subscription['DiplomaDate'] = pd.to_datetime(fact_subscription['DiplomaDate'], errors='coerce')
    fact_subscription = fact_subscription.merge(dim_time[['Date', 'TimeID']], left_on='SubscriptionStartDate', right_on='Date', how='left').rename(columns={'TimeID': 'StartTimeID'}).drop('Date', axis=1)
    fact_subscription = fact_subscription.merge(dim_time[['Date', 'TimeID']], left_on='SubscriptionEndDate', right_on='Date', how='left').rename(columns={'TimeID': 'EndTimeID'}).drop('Date', axis=1)
    fact_subscription = fact_subscription.merge(dim_time[['Date', 'TimeID']], left_on='DiplomaDate', right_on='Date', how='left').rename(columns={'TimeID': 'DiplomaTimeID'}).drop('Date', axis=1)
    
    fact_subscription['SubscriptionID'] = fact_subscription.index + 1
    fact_subscription = fact_subscription[['SubscriptionID', 'CourseOfferingID', 'StudentID', 'StartTimeID', 'EndTimeID', 'DiplomaTimeID', 'SubscriptionProgress', 'SubscriptionHasDiploma']]
    
    # Load data using pandas.to_sql with SQLAlchemy engine in dependency order
    dim_student.to_sql('dim_student', engine, schema=SCHEMA, if_exists='append', index=False)
    dim_instructor.to_sql('dim_instructor', engine, schema=SCHEMA, if_exists='append', index=False)
    dim_course_offering.to_sql('dim_course_offering', engine, schema=SCHEMA, if_exists='append', index=False)
    dim_time.to_sql('dim_time', engine, schema=SCHEMA, if_exists='append', index=False)
    fact_subscription.to_sql('fact_subscription', engine, schema=SCHEMA, if_exists='append', index=False)
    
    print(f"Processed {CSV_FILE} and loaded into PostgreSQL.")

if __name__ == "__main__":
    process_csv()