"""
Chicago Crime Data Pipeline with Soda Quality Checks
Simple and professional data pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json
import pandas as pd
import os

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# File paths
RAW_DATA_PATH = '/tmp/raw_data.json'
CLEAN_DATA_PATH = '/tmp/clean_data.csv'

# Database connection
DB_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5432,
    'database': 'postgres',
    'user': 'admin',
    'password': 'abEEE!%!^!!11'
}

# ========== DATA QUALITY STORAGE ==========
def store_dq_result(pipeline_name, step_name, check_type, status, records_checked, issues_found=0, details=""):
    """Store data quality results in PostgreSQL"""
    try:
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        from sqlalchemy import create_engine, text
        engine = create_engine(conn_string)
        
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO dq_results (pipeline_name, step_name, check_type, status, records_checked, issues_found, details)
                VALUES (:pipeline_name, :step_name, :check_type, :status, :records_checked, :issues_found, :details)
            """), {
                'pipeline_name': pipeline_name,
                'step_name': step_name,
                'check_type': check_type,
                'status': status,
                'records_checked': records_checked,
                'issues_found': issues_found,
                'details': details
            })
            conn.commit()
        
        print(f"✅ DQ Result stored: {step_name} - {status}")
        
    except Exception as e:
        print(f"❌ Error storing DQ result: {str(e)}")

# ========== TASK 1: DATA INGESTION ==========
def ingest_data():
    """Fetch crime data from Chicago API"""
    api_url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=20000"
    
    print(f"Fetching data from: {api_url}")
    response = requests.get(api_url, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    print(f"Successfully fetched {len(data)} records")
    
    with open(RAW_DATA_PATH, 'w') as f:
        json.dump(data, f)
    
    print(f"Raw data saved to: {RAW_DATA_PATH}")

# ========== TASK 2: SODA QUALITY CHECK RAW ==========
def soda_check_raw():
    """Run Soda quality checks on raw data"""
    try:
        # Load raw data to temporary table for Soda
        with open(RAW_DATA_PATH, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        
        # Create temporary table
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        from sqlalchemy import create_engine
        engine = create_engine(conn_string)
        
        df.to_sql('temp_raw_data', engine, if_exists='replace', index=False)
        
        # Run Soda check
        import subprocess
        result = subprocess.run([
            'soda', 'scan',
            '-d', 'postgres',
            '-c', '/opt/airflow/include/configuration.yml',
            '/opt/airflow/include/checks_raw.yml'
        ], capture_output=True, text=True)
        
        # Store results
        if result.returncode == 0:
            store_dq_result('chicago_crime_pipeline', 'raw_data_check', 'soda_validation', 'PASS', len(df), 0, 'All raw data quality checks passed')
        else:
            store_dq_result('chicago_crime_pipeline', 'raw_data_check', 'soda_validation', 'FAIL', len(df), 1, result.stderr)
        
        print(f"Soda raw check result: {result.stdout}")
        
    except Exception as e:
        store_dq_result('chicago_crime_pipeline', 'raw_data_check', 'soda_validation', 'ERROR', 0, 1, str(e))
        raise

# ========== TASK 3: DATA TRANSFORMATION ==========
def transform_data():
    """Clean and transform the raw data"""
    print("Loading raw data...")
    with open(RAW_DATA_PATH, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    print(f"Loaded {len(df)} records")
    
    # Clean data
    df_clean = df.copy()
    
    # Remove rows with missing critical fields
    critical_fields = ['id', 'case_number', 'date']
    df_clean = df_clean.dropna(subset=critical_fields)
    
    # Keep relevant columns
    columns_to_keep = [
        'id', 'case_number', 'date', 'block', 'primary_type',
        'description', 'location_description', 'arrest', 
        'domestic', 'year', 'latitude', 'longitude'
    ]
    
    columns_to_keep = [col for col in columns_to_keep if col in df_clean.columns]
    df_clean = df_clean[columns_to_keep]
    
    # Convert boolean fields
    if 'arrest' in df_clean.columns:
        df_clean['arrest'] = df_clean['arrest'].astype(str).str.lower() == 'true'
    if 'domestic' in df_clean.columns:
        df_clean['domestic'] = df_clean['domestic'].astype(str).str.lower() == 'true'
    
    print(f"Cleaned data: {len(df_clean)} records")
    
    # Save cleaned data
    df_clean.to_csv(CLEAN_DATA_PATH, index=False)
    print(f"Clean data saved to: {CLEAN_DATA_PATH}")

# ========== TASK 4: SODA QUALITY CHECK CLEAN ==========
def soda_check_clean():
    """Run Soda quality checks on clean data"""
    try:
        # Load clean data to temporary table
        df = pd.read_csv(CLEAN_DATA_PATH)
        
        # Create temporary table
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        from sqlalchemy import create_engine
        engine = create_engine(conn_string)
        
        df.to_sql('temp_clean_data', engine, if_exists='replace', index=False)
        
        # Run Soda check
        import subprocess
        result = subprocess.run([
            'soda', 'scan',
            '-d', 'postgres',
            '-c', '/opt/airflow/include/configuration.yml',
            '/opt/airflow/include/checks_clean.yml'
        ], capture_output=True, text=True)
        
        # Store results
        if result.returncode == 0:
            store_dq_result('chicago_crime_pipeline', 'clean_data_check', 'soda_validation', 'PASS', len(df), 0, 'All clean data quality checks passed')
        else:
            store_dq_result('chicago_crime_pipeline', 'clean_data_check', 'soda_validation', 'FAIL', len(df), 1, result.stderr)
        
        print(f"Soda clean check result: {result.stdout}")
        
    except Exception as e:
        store_dq_result('chicago_crime_pipeline', 'clean_data_check', 'soda_validation', 'ERROR', 0, 1, str(e))
        raise

# ========== TASK 5: LOAD TO DATABASE ==========
def load_to_database():
    """Load cleaned data to PostgreSQL"""
    print("Loading clean data from CSV...")
    df = pd.read_csv(CLEAN_DATA_PATH)
    print(f"Loaded {len(df)} records from CSV")
    
    conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine(conn_string)
        
        # Ensure data types are correct
        df['id'] = df['id'].astype(str)
        df['date'] = pd.to_datetime(df['date'])
        
        # Load data to PostgreSQL
        print(f"Loading {len(df)} records to chicago_crimes table...")
        df.to_sql('chicago_crimes', engine, if_exists='replace', index=False)
        
        print("✅ Data successfully loaded to PostgreSQL!")
        print(f"📊 Table: chicago_crimes")
        print(f"📈 Records: {len(df)}")
        
    except Exception as e:
        print(f"❌ Error loading data: {str(e)}")
        raise

# ========== DAG DEFINITION ==========
with DAG(
    'chicago_crime_pipeline',
    default_args=default_args,
    description='Pipeline for Chicago crime data with Soda quality checks',
    schedule_interval='0 1 * * *',  # Run daily at 01:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['chicago', 'crime', 'soda', 'quality'],
) as dag:
    
    # Task 1: Ingest data from API
    task_ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
    )
    
    # Task 2: Soda quality check on raw data
    task_soda_raw = PythonOperator(
        task_id='soda_check_raw',
        python_callable=soda_check_raw,
    )
    
    # Task 3: Transform data
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    # Task 4: Soda quality check on clean data
    task_soda_clean = PythonOperator(
        task_id='soda_check_clean',
        python_callable=soda_check_clean,
    )
    
    # Task 5: Load data to PostgreSQL
    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_database,
    )
    
    # Define task dependencies
    task_ingest >> task_soda_raw >> task_transform >> task_soda_clean >> task_load