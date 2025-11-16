"""
Example Airflow DAG for Weather + Air Quality ETL Pipeline.

This is a skeleton DAG that demonstrates how to schedule the weather ETL
process using Apache Airflow. This file serves as a template and requires
Airflow to be installed and configured.

To use this DAG:
1. Install Airflow with: pip install "apache-airflow>=2.7.0"
2. Copy this file to your Airflow DAGs directory
3. Configure Airflow connections for your database
4. Enable the DAG in the Airflow UI
"""

from datetime import datetime, timedelta
from typing import Dict, Any

# Airflow imports (commented out to avoid import errors when Airflow is not installed)
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# Project imports
from src.etl.fetch_and_load_weather import WeatherETL
from src.etl.transform_to_analytics import WeatherDataTransformer


# DAG configuration
DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

DAG_ID = 'weather_aqi_etl_pipeline'
SCHEDULE_INTERVAL = '0 */6 * * *'  # Every 6 hours

# Uncomment and modify when Airflow is available
"""
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Weather and Air Quality ETL Pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    tags=['weather', 'air-quality', 'etl'],
)
"""


def fetch_and_load_weather_data(**context) -> Dict[str, Any]:
    """
    Airflow task function to fetch and load weather data.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        ETL summary dictionary
    """
    print("Starting weather data ETL task...")
    
    etl = WeatherETL()
    summary = etl.run_etl()
    
    # Log results for Airflow
    if summary['status'] == 'success':
        print(f"✅ ETL completed successfully:")
        print(f"   Duration: {summary['duration_seconds']:.2f} seconds")
        print(f"   Records loaded: {summary['records_loaded']}")
        print(f"   Weather successes: {summary['weather_successes']}")
        print(f"   AQI successes: {summary['aqi_successes']}")
    else:
        print(f"❌ ETL failed: {summary['error']}")
        raise Exception(f"ETL process failed: {summary['error']}")
    
    return summary


def run_data_quality_checks(**context) -> Dict[str, Any]:
    """
    Airflow task function to run data quality checks.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Data quality report
    """
    print("Running data quality checks...")
    
    transformer = WeatherDataTransformer()
    quality_report = transformer.run_data_quality_check(limit=50)
    
    if 'error' in quality_report:
        print(f"❌ Data quality check failed: {quality_report['error']}")
        raise Exception(f"Data quality check failed: {quality_report['error']}")
    
    # Check quality threshold
    quality_threshold = 80.0  # 80% minimum quality score
    quality_score = quality_report['data_quality_score']
    
    if quality_score < quality_threshold:
        print(f"⚠️  Data quality below threshold: {quality_score}% < {quality_threshold}%")
        # In production, you might want to send alerts here
    else:
        print(f"✅ Data quality check passed: {quality_score}%")
    
    print(f"   Records checked: {quality_report['total_records_checked']}")
    print(f"   Valid records: {quality_report['valid_records']}")
    
    return quality_report


def run_dbt_transformations(**context) -> str:
    """
    Airflow task function to run dbt transformations.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Success message
    """
    print("Running dbt transformations...")
    
    # In a real Airflow environment, you would use BashOperator or 
    # a dedicated dbt operator to run dbt commands
    # For now, this is a placeholder
    
    print("✅ dbt transformations completed (placeholder)")
    return "dbt transformations completed"


def send_success_notification(**context) -> None:
    """
    Airflow task function to send success notification.
    
    Args:
        context: Airflow context dictionary
    """
    print("Sending success notification...")
    
    # Get ETL summary from previous task
    etl_summary = context['task_instance'].xcom_pull(task_ids='extract_load_weather_data')
    quality_report = context['task_instance'].xcom_pull(task_ids='data_quality_checks')
    
    message = f"""
    🌤️ Weather ETL Pipeline Completed Successfully
    
    📊 ETL Summary:
    - Records loaded: {etl_summary.get('records_loaded', 'N/A')}
    - Duration: {etl_summary.get('duration_seconds', 'N/A')} seconds
    - Weather API successes: {etl_summary.get('weather_successes', 'N/A')}
    - AQI API successes: {etl_summary.get('aqi_successes', 'N/A')}
    
    ✅ Data Quality:
    - Quality score: {quality_report.get('data_quality_score', 'N/A')}%
    - Records checked: {quality_report.get('total_records_checked', 'N/A')}
    
    Execution Date: {context['ds']}
    """
    
    print(message)
    
    # In production, you would send this via email, Slack, etc.
    # Example integrations:
    # - EmailOperator
    # - SlackAPIPostOperator
    # - Custom webhook calls


# Uncomment and modify when Airflow is available
"""
# Define tasks
extract_load_task = PythonOperator(
    task_id='extract_load_weather_data',
    python_callable=fetch_and_load_weather_data,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

dbt_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command='cd /app/dbt_weather && dbt run',
    dag=dag,
)

# Alternative: Use PostgresOperator for simple SQL operations
# cleanup_old_data_task = PostgresOperator(
#     task_id='cleanup_old_data',
#     postgres_conn_id='weather_postgres',
#     sql=\"\"\"
#         DELETE FROM raw.weather_observations 
#         WHERE created_at < NOW() - INTERVAL '30 days'
#     \"\"\",
#     dag=dag,
# )

notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Define task dependencies
extract_load_task >> quality_check_task >> dbt_task >> notification_task
"""


def create_dag_example():
    """
    Function to demonstrate DAG creation when Airflow is available.
    
    This function shows the structure of how the DAG would be created
    in a real Airflow environment.
    """
    dag_code = '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Create DAG
dag = DAG(
    'weather_aqi_etl_pipeline',
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Weather and Air Quality ETL Pipeline',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['weather', 'air-quality', 'etl'],
)

# Define tasks
extract_load = PythonOperator(
    task_id='extract_load_weather_data',
    python_callable=fetch_and_load_weather_data,
    dag=dag,
)

quality_check = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /app/dbt_weather && dbt run',
    dag=dag,
)

# Task dependencies
extract_load >> quality_check >> dbt_run
'''
    return dag_code


if __name__ == "__main__":
    print("🚀 Airflow DAG Example for Weather ETL Pipeline")
    print("\nThis is a template DAG file. To use with Airflow:")
    print("1. Install Airflow: pip install apache-airflow")
    print("2. Initialize Airflow database: airflow db init")
    print("3. Copy this file to your DAGs directory")
    print("4. Start Airflow: airflow webserver & airflow scheduler")
    print("\nDAG Configuration:")
    print(f"   DAG ID: {DAG_ID}")
    print(f"   Schedule: {SCHEDULE_INTERVAL}")
    print(f"   Start Date: {DEFAULT_ARGS['start_date']}")
    
    print("\n📋 Task Flow:")
    print("   1. Extract & Load Weather Data")
    print("   2. Run Data Quality Checks")
    print("   3. Run dbt Transformations")
    print("   4. Send Success Notification")
    
    # Test the task functions without Airflow
    print("\n🧪 Testing task functions (without Airflow)...")
    try:
        # Test ETL function
        etl_result = fetch_and_load_weather_data()
        print(f"ETL test result: {etl_result.get('status', 'unknown')}")
        
        # Test quality check function
        quality_result = run_data_quality_checks()
        print(f"Quality check test result: {quality_result.get('data_quality_score', 'unknown')}% quality")
        
    except Exception as e:
        print(f"❌ Task testing failed: {e}")
        print("This is expected if database is not available or APIs are not configured.")