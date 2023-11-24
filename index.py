
import configparser
import pandas as pd
import psycopg2
import logging 
from sqlalchemy import create_engine

from utils.helper import create_bucket
config= configparser.ConfigParser()
config.read('.env')

access_key= config['AWS']['access_key']
secret_key= config['AWS']['secret_key']
bucket_name= config['AWS']['bucket_name']
region= config['AWS']['region']

db_host= config['DB_CONN']['host']
db_user= config['DB_CONN']['user']
db_password= config['DB_CONN']['password']
db_database= config['DB_CONN']['database']

# creating S3 Bucket using boto3

##create_bucket(access_key, secret_key, bucket_name, region)

#connect to local database and extract  data from postgresql to datalake
conn= create_engine('postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_database}')
c=conn.connect()
db_tables= ['cleaned_call_details','cleaned_call_log']

for table in db_tables:
    query = f'SELECT * FROM {table}'
    logging.info('======Executing {query}')
    df= pd.read_sql_query(query,conn)

    df.to_csv(
        f's3://{bucket_name}/{table}.csv'
        ,index=False
        ,storage_options={
            'key': access_key,
            'secret': secret_key
        }
        
    )