import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.sql import text
import argparse
import sys

# Load environment variables from .env file
load_dotenv()

# Configuration validation
def validate_config():
    required = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'SCHEMA']
    config = {key: os.getenv(key) for key in required}
    missing = [key for key, value in config.items() if value is None]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    config['DB_PORT'] = int(config['DB_PORT'])  # Convert to integer
    return config

# Parse command-line arguments
parser = argparse.ArgumentParser(description="ETL Pipeline for GoMyCode DataOps Task")
parser.add_argument('--csv-path', type=str, help="Path to the CSV file", default='Functional Task - OLTP_Subscription.csv')
args = parser.parse_args()

# Configuration from environment variables
config = validate_config()
DB_USER = config['DB_USER']
DB_PASSWORD = config['DB_PASSWORD']
DB_HOST = config['DB_HOST']
DB_PORT = config['DB_PORT']
DB_NAME = config['DB_NAME']
SCHEMA = config['SCHEMA']

# CSV file path
script_dir = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(script_dir, args.csv_path) if args.csv_path else os.path.join(script_dir, 'Functional Task - OLTP_Subscription.csv')

# Create database engine
def get_engine():
    try:
        engine_url = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding=utf8'
        return create_engine(engine_url)
    except Exception as e:
        raise ConnectionError(f"Failed to create database engine: {e}")

# Drop and recreate schema
def reset_schema(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;"))
            connection.execute(text(f"CREATE SCHEMA {SCHEMA};"))
            connection.commit()
        print("Schema reset successfully.")
    except Exception as e:
        raise RuntimeError(f"Schema reset failed: {e}")

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
          Column('SubscriptionProgress', Float),  # Stored as 0 to 1
          Column('SubscriptionHasDiploma', Integer))  # Stored as 0 or 1
    
    try:
        metadata.create_all(engine)
        print("Tables created successfully.")
    except Exception as e:
        raise RuntimeError(f"Table creation failed: {e}")

def process_csv():
    engine = get_engine()
    reset_schema(engine)
    create_tables(engine)
    
    # Extract
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"Error: CSV file {CSV_FILE} not found.")
    
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_FILE, encoding='cp1252')
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV: {e}")
    
    # Validate required columns
    required_columns = ['Student', 'InstructorFullName', 'GroupName', 'SubscriptionStartDate', 'SubscriptionProgress', 'SubscriptionHasDiploma']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Clean data: handle missing values with mean/median/mode
    try:
        df['professionalExperience'] = df['professionalExperience'].fillna(df['professionalExperience'].median())
        df['StudentGender'] = df['StudentGender'].fillna(df['StudentGender'].mode()[0])
        df['Industry'] = df['Industry'].fillna(df['Industry'].mode()[0])
        date_columns = ['StudentBirthDate', 'SubscriptionStartDate', 'SubscriptionEndDate', 'DiplomaDate']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            median_date = df[col].median()
            df[col] = df[col].fillna(median_date)
        df['SubscriptionProgress'] = df['SubscriptionProgress'].fillna('0%').str.rstrip('%').astype(float) / 100.0
        print("Raw SubscriptionHasDiploma values:", df['SubscriptionHasDiploma'].head())
        df['SubscriptionHasDiploma'] = df['SubscriptionHasDiploma'].fillna(False).astype(int)
        print("Transformed SubscriptionHasDiploma values:", df['SubscriptionHasDiploma'].head())
    except Exception as e:
        raise ValueError(f"Data cleaning failed: {e}")

    # Transform
    try:
        dim_instructor = df[['InstructorFullName', 'InstructorEmail', 'instructor_diploma']].drop_duplicates().reset_index(drop=True)
        dim_instructor['InstructorID'] = dim_instructor.index + 1
        
        course_offering_key = ['GroupName', 'SessionName', 'TrackName', 'Hackerspace', 'Country', 'ProductSchedule']
        dim_course_offering = df[course_offering_key + ['InstructorFullName', 'InstructorEmail']].drop_duplicates().reset_index(drop=True)
        dim_course_offering = dim_course_offering.merge(dim_instructor[['InstructorFullName', 'InstructorEmail', 'InstructorID']], 
                                                      on=['InstructorFullName', 'InstructorEmail'], how='left')
        dim_course_offering = dim_course_offering[course_offering_key + ['InstructorID']].reset_index(drop=True)
        dim_course_offering['CourseOfferingID'] = dim_course_offering.index + 1
        
        dim_student = df[['Student', 'StudentGender', 'StudentBirthDate', 'professionalExperience', 'Industry']].drop_duplicates().reset_index(drop=True)
        dim_student['StudentID'] = dim_student.index + 1
        
        dates = pd.concat([df['SubscriptionStartDate'], df['SubscriptionEndDate'], df['DiplomaDate']]).dropna().unique()
        dim_time = pd.DataFrame({'Date': pd.to_datetime(dates, errors='coerce')})
        dim_time['TimeID'] = dim_time.index + 1
        dim_time['Year'] = dim_time['Date'].dt.year
        dim_time['Month'] = dim_time['Date'].dt.month
        dim_time['Day'] = dim_time['Date'].dt.day
        
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
    except Exception as e:
        raise RuntimeError(f"Data transformation failed: {e}")

    # Load data using pandas.to_sql with SQLAlchemy engine
    try:
        dim_student.to_sql('dim_student', engine, schema=SCHEMA, if_exists='append', index=False)
        dim_instructor.to_sql('dim_instructor', engine, schema=SCHEMA, if_exists='append', index=False)
        dim_course_offering.to_sql('dim_course_offering', engine, schema=SCHEMA, if_exists='append', index=False)
        dim_time.to_sql('dim_time', engine, schema=SCHEMA, if_exists='append', index=False)
        fact_subscription.to_sql('fact_subscription', engine, schema=SCHEMA, if_exists='append', index=False)
        print(f"Processed {CSV_FILE} and loaded into PostgreSQL.")
    except Exception as e:
        raise RuntimeError(f"Data loading failed: {e}")

    # Now export the flattened CSV for direct Looker Studio use
    export_flattened_csv(dim_student, dim_instructor, dim_course_offering, dim_time, fact_subscription)

def export_flattened_csv(dim_student, dim_instructor, dim_course_offering, dim_time, fact_subscription):
    # Join fact_subscription with dims to get descriptive fields
    
    # Join fact with dim_course_offering
    fact_dim_course = fact_subscription.merge(
        dim_course_offering, on='CourseOfferingID', how='left'
    )
    
    # Join with dim_student
    fact_dim_course_student = fact_dim_course.merge(
        dim_student, on='StudentID', how='left'
    )
    
    # Join with dim_time for start, end, diploma dates
    # Rename dim_time columns for each time id
    dim_time_start = dim_time.rename(columns={
        'TimeID': 'StartTimeID',
        'Date': 'SubscriptionStartDate',
        'Year': 'StartYear',
        'Month': 'StartMonth',
        'Day': 'StartDay'
    })
    
    dim_time_end = dim_time.rename(columns={
        'TimeID': 'EndTimeID',
        'Date': 'SubscriptionEndDate',
        'Year': 'EndYear',
        'Month': 'EndMonth',
        'Day': 'EndDay'
    })
    
    dim_time_diploma = dim_time.rename(columns={
        'TimeID': 'DiplomaTimeID',
        'Date': 'DiplomaDate',
        'Year': 'DiplomaYear',
        'Month': 'DiplomaMonth',
        'Day': 'DiplomaDay'
    })
    
    df = fact_dim_course_student.merge(
        dim_time_start[['StartTimeID', 'SubscriptionStartDate', 'StartYear', 'StartMonth', 'StartDay']],
        on='StartTimeID', how='left'
    ).merge(
        dim_time_end[['EndTimeID', 'SubscriptionEndDate', 'EndYear', 'EndMonth', 'EndDay']],
        on='EndTimeID', how='left'
    ).merge(
        dim_time_diploma[['DiplomaTimeID', 'DiplomaDate', 'DiplomaYear', 'DiplomaMonth', 'DiplomaDay']],
        on='DiplomaTimeID', how='left'
    )
    
    # Drop IDs if not needed, keep descriptive columns
    drop_cols = ['SubscriptionID', 'CourseOfferingID', 'StudentID', 'StartTimeID', 'EndTimeID', 'DiplomaTimeID']
    df = df.drop(columns=drop_cols, errors='ignore')
    
    # Reorder columns if you want (example)
    cols_order = [
        'Student', 'StudentGender', 'StudentBirthDate', 'professionalExperience', 'Industry',
        'GroupName', 'SessionName', 'TrackName', 'Hackerspace', 'Country', 'ProductSchedule',
        'InstructorID', 'InstructorFullName', 'InstructorEmail', 'instructor_diploma',
        'SubscriptionStartDate', 'StartYear', 'StartMonth', 'StartDay',
        'SubscriptionEndDate', 'EndYear', 'EndMonth', 'EndDay',
        'DiplomaDate', 'DiplomaYear', 'DiplomaMonth', 'DiplomaDay',
        'SubscriptionProgress', 'SubscriptionHasDiploma'
    ]
    cols_order = [c for c in cols_order if c in df.columns]
    df = df[cols_order]
    
    # Export flattened CSV
    output_path = os.path.join(script_dir, 'flattened_subscription_data.csv')
    df.to_csv(output_path, index=False)
    print(f"Flattened CSV exported to {output_path}")

if __name__ == "__main__":
    try:
        process_csv()
    except Exception as e:
        print(f"ETL process failed: {e}", file=sys.stderr)
        sys.exit(1)
