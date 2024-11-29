from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.decoators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from .Weather_Extractor import WeatherExtractor
from .Weather_Transformer import WeatherTransformer
from .Weather_Loader import WeatherLoader


class WeatherPipeline:

    def __init__(self, API_url, API_key, db_path):
        self.API_url = API_url
        self.API_key = API_key
        self.db_path = db_path
        self.extractor = WeatherExtractor(self.API_key, self.API_url)
        self.transformer = WeatherTransformer()
        self.loader = WeatherLoader(self.db_path)

    def run_pipeline(self, location):
        # Extract data from source
        # data = self.extractor.fetch_weather_data(location)
        # Transform
        # transformed_data = self.transformer.transform_data(data)
        # Load data to database
        # self.loader.insert_weather_data(location, transformed_data)

        # self.loader.quruer_location()
        # self.loader.quruer_weather_data()

        # Load data from database to analysis
        # return self.loader.quruer_weather_date_location(location)
        # self.loader.close_connection()
        # Analyze

        # Default arguments for Airflow DAG
        default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        }

        # Define the DAG
        with DAG(
            "weather_pipeline_dag",
            default_args=default_args,
            description="Weather ETL pipeline",
            schedule_interval=timedelta(hours=3),  # Runs every 3 hours
            start_date=datetime(2024, 11, 29),
            catchup=False,
        ) as dag:

            # Define each pipeline task as a function
            def extract_task():
                data = self.extractor.fetch_weather_data(location)
                return data  # Use Airflow's XCom to pass this data to the next task

            def transform_task(ti):
                data = ti.xcom_pull(task_ids="extract")  # Retrieve from previous step
                transformed_data = self.transformer.transform_data(data)
                return transformed_data

            def load_task(ti):
                transformed_data = ti.xcom_pull(task_ids="transform")
                self.loader.insert_weather_data(location, transformed_data)

            # Define Airflow tasks using PythonOperator
            extract = PythonOperator(
                task_id="extract",
                python_callable=extract_task,
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_task,
            )

            load = PythonOperator(
                task_id="load",
                python_callable=load_task,
            )

            # Set task dependencies (sequence of execution)
            extract >> transform >> load

            return self.loader.quruer_weather_date_location(location)
