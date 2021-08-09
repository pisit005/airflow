import datetime
from typing import Collection
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from pymongo import MongoClient
import pymongo


class MongoHook(BaseHook):

    conn_name_attr = 'MongoDB'
    default_conn_name = 'MongoDB'
    conn_type = 'mongo'
    hook_name = 'MongoDB'

    def __init__(self, conn_id: str = default_conn_name) -> None:

            super().__init__()
            self.mongo_conn_id = conn_id
            self.connection = self.get_connection(conn_id)
            self.extras = self.connection.extra_dejson.copy()
            self.client = None

            srv = self.extras.pop('srv', False)
            schema = 'mongodb+srv' if srv else 'mongodb'

            self.uri = 'mongodb://localhost:root@cluster0-shard-00-00.q9nwx.mongodb.net:27017,cluster0-shard-00-01.q9nwx.mongodb.net:27017,cluster0-shard-00-02.q9nwx.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-odcqvu-shard-0&authSource=admin&retryWrites=true&w=majority'.format(
                schema=schema,
                creds=f'{self.connection.login}:{self.connection.password}@' if self.connection.login else '',
                host=self.connection.host,
                port='' if self.connection.port is None else f':{self.connection.port}',
                database=self.connection.schema,
            )

    def get_conn(self
                     ) -> MongoClient:
            if self.client is not None:
                return self.client

            return self.client

    def get_collection(self, mongo_collection: str, mongo_db: str
                           ) -> pymongo.collection.Collection:
            mongo_db = mongo_db if mongo_db is not None else self.connection.schema
            mongo_conn: MongoClient = self.get_conn()

            for x in mongo_collection.find():
                print(x)
            print('error')

            return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def insert(self, mongo_collection: str, docs: dict, mongo_db: str
                   ) -> pymongo.results.InsertManyResult:

            docs = {"name": "John", "address": "Highway 37"}

            collection = self.get_collection(
                mongo_collection, mongo_db=mongo_db)
            return collection.insert_many(docs)
