#!/usr/bin/env python
# coding: utf-8
import os
import argparse

import pandas as pd
from sqlalchemy import create_engine
from time import time

# get_ipython().system('pip install psycopg2-binary')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # download the csv
    os.system(f"wget {url} -O {csv_name}")
    
    # engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # query = """
    # SELECT 1 as number;
    # """

    # pd.read_sql(query, con=engine)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
        
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
        
            t_end = time()
            print('Inserted another chunk..., took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

    # query = """
    # select * 
    # from pg_catalog.pg_tables
    # where schemaname != 'pg_catalog' AND
    #     schemaname != 'information_schema';
    # """

    # pd.read_sql(query, con=engine)
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    # user, password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', required=True, help='User name for Postgres')
    parser.add_argument('--password', required=True, help='password for Postgres')
    parser.add_argument('--host', required=True, help='host for Postgres')
    parser.add_argument('--port', required=True, help='port for Postgres')
    parser.add_argument('--db', required=True, help='database name for Postgres')
    parser.add_argument('--table_name', required=True, help='table name for Postgres where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)



