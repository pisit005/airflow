import datetime
from typing import Collection
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from pymongo import MongoClient
import pymongo

with DAG(
    '123456789',
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",

)as dag:
    t1 = MongoHook(
        task_id="create_mongo_table",
        mongo_conn_id = "MongoDB",
        conn_name_attr = 'MongoDB',
        default_conn_name = 'MongoDB',
        conn_type = 'mongo',
        
    
        


    )
