1. this project should use library, please do install this library before run this project :

import logging
import requests
import pendulum
import pandas as pd
from tqdm import tqdm
from datetime import timedelta
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


2. I'am using Apache Airflow as the orchestrator tools to get data every 5 minutes, batch pipeline.
   Settings that i used in my Local Apache Airflow is :

   Pool = {"Pool Name" : "xpools", "Pool Workers" : 4}
   Connection = Connection to PostgreSQL DB to store datas

   and the library parameters I used:

   from airflow import DAG
   from airflow.models import XCom
   from airflow.utils.db import provide_session
   from airflow.operators.dummy import DummyOperator
   from airflow.operators.python import PythonOperator
   from airflow.hooks.postgres_hook import PostgresHook

3. I'am using PostgreSQL as DB, in this DB I created a table:

    create table if not exists public.x_tax(
        "timestamp" timestamp,
        latitude double precision,
        longitude double precision,
        taxi_position varchar,
        taxi_position_y varchar,
        taxi_areas_pos varchar
    );