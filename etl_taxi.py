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

def get_street(x):
    
    try:
        if int(x.split(",")[0]):
            street = x.split(",")[1].strip()
    except ValueError as ve:
        street = x.split(",")[0].strip()
        
    return street

def taxi_areas_position(x):
    if x.split(",")[-4].strip() == 'Singapore':
        taxi_areas_pos = x.split(",")[-5]
    else:
        taxi_areas_pos = x.split(",")[-4]
    
    return taxi_areas_pos

def extract_transform_load_data():
    
    lati = []
    longi = []
    coordinates = []

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
        
        ## convert coordinate to list
        coordinate_list = df['geometry.coordinates'].tolist()

        df1 = pd.DataFrame(coordinate_list)

        ## step to get coordinates, latitude, longitude and timestamp
        result = df1.transpose()

        ## get coordinates
        result.columns = ['coordinates']

        logger.info("get the coordinates")

        ## get timestamp
        result['timestamp'] = (df['properties.timestamp'][0])

        data_taxi = pd.DataFrame()

        d_taxi = data_taxi.append(result)
        
        d_taxi['coordinates'] = d_taxi['coordinates'].astype(str)

        logger.info("get latitude and longitude")

        ## get latitude and longitude
        for i in d_taxi['coordinates']:
            i = i.split(",")

            lat = i[1][:-1]
            long = i[0][1:]

            lati.append(lat)
            longi.append(long)

        d_taxi['latitude'] = lati
        d_taxi['longitude'] = longi
        
        ## create new column to get exactly location
        d_taxi["latlong_location"] = d_taxi["latitude"].astype(str) + "," + d_taxi["longitude"].astype(str)
        
        ## get exactly location from latitude and longitude
        logger.info("get exactly location from latitude dan longitude")
        geo_loc = Nominatim(user_agent="GetLoc")
        geo_code = RateLimiter(geo_loc.reverse, min_delay_seconds=0.001)
        
        d_taxi["taxi_position"] = d_taxi["latlong_location"].apply(geo_code)
        
        ## remove specific unused character
        logger.info("cleaning columns taxi_position and timestamp")
        d_taxi["taxi_position"] = d_taxi["taxi_position"].astype(str).map(lambda x: x.replace('(', '').replace(')', ''))
        d_taxi["timestamp"] = d_taxi["timestamp"].astype(str).map(lambda x: x.replace('T', ' '))
        
        ## remove `coordinates` and `latlong_location` columns
        d_taxi.drop(columns=["coordinates", "latlong_location"], inplace=True)
        
        ## get taxi position by location address
        d_taxi["taxi_position_x"] = d_taxi["taxi_position"].map(lambda x: x.rsplit(",", 1)[-2])
        d_taxi["taxi_position_y"] = d_taxi["taxi_position_x"].map(lambda x: x.rsplit(",", 1)[-2])
        d_taxi["taxi_position_y"] = d_taxi["taxi_position_x"].map(lambda x: get_street(x))
        
        ## afther get taxi position by loacation address, remove unused columns `taxi_position_x`
        d_taxi.drop(columns=["taxi_position_x"], inplace=True)
        
        ## get taxi area position
        logger.info("get taxi area")
        d_taxi["taxi_areas_pos"] = d_taxi["taxi_position"].map(lambda x: taxi_areas_position(x))
        
        logger.info(d_taxi.head())

        logger.info("load data to db")
        
        ## load data to postgresql
        d_taxi.to_sql(
            "x_tax",
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
    dag_id = 'extract_datas_and_load',
    default_args = default_args,
    start_date = pendulum.datetime(2022, 9, 1, tz="Asia/Singapore"),
    schedule_interval='*/5 * * * *',
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