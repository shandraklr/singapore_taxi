import logging
import requests
import pendulum
import pandas as pd
from tqdm import tqdm
from datetime import timedelta
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


tqdm.pandas()

today_minus_two_hour = pendulum.now(tz = "Asia/Singapore") - timedelta(minutes = 2)
today_minus_two_hour = today_minus_two_hour.strftime("%Y-%m-%dT%H:%m")

logger = logging.getLogger(__name__)

def extract_transform_load_data():

    dest = PostgresHook(postgres_conn_id='DB_ANALYTICS')
    dest_engine = dest.get_sqlalchemy_engine()
    
    url = f"https://api.data.gov.sg/v1/transport/taxi-availability?date_time={today_minus_two_hour}:00"
    
    credit = requests.get(url)
    
    ## checking if status 200 [OK] or not
    logger.info("Checking Status Code")
    if credit.status_code == 200:
        
        logger.info("Status OKE, extraction Data")

        ## dump to json format
        data = credit.json()
        
        ## normalize json format
        df = pd.json_normalize(data['features'])

        df["timestamp"] = df["properties.timestamp"]
        df["taxi_count_availability"] = df["properties.taxi_count"]

        df_taxi = df.copy()

        df_taxi.drop(columns=["type", "geometry.type", "geometry.coordinates", "properties.timestamp", "properties.taxi_count", "properties.api_info.status"], inplace=True)
        
        df_taxi["timestamp"] = df_taxi["timestamp"].astype(str).map(lambda x: x.replace('T', ' '))

        logger.info("load data to db")
        
        ## load data to postgresql
        df_taxi.to_sql(
            "x_tax_availability",
            dest_engine,
            if_exists = "append",
            index = False,
            schema = "public"
        )
        
    else:
        print("Request failed with status code:", credit.status_code)
    


@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


default_args = {
    'owner' : 'shadrak',
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}

with DAG(
    dag_id = 'extract_datas_and_load_taxi_availability',
    default_args = default_args,
    start_date = pendulum.datetime(2022, 9, 1, tz="Asia/Singapore"),
    schedule_interval='0 * * * *',
    catchup=False,
    tags = ["data extraction"]
) as dag:

    start_task = DummyOperator(
        task_id = "start"
    )

    extracting = PythonOperator(
        task_id = "get_and_loaded_data",
        python_callable = extract_transform_load_data,
        pool = "xpools",
        do_xcom_push = False
    )

    delete_xcom = PythonOperator(
        task_id = "delete_xcom",
        python_callable = cleanup_xcom,
        provide_context = True
    )

    end_task = DummyOperator(
        task_id = "end"
    )

    start_task >> extracting >> delete_xcom >> end_task