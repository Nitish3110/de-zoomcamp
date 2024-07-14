import argparse, os, sys

import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine


def ingest(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'nyc_data.csv.gz' if url.endswith('.csv.gz') else 'nyc_data.csv'
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgressql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #create table schema, if exists : replace
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #insert first chuck
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('Chunk Inserted, took %.3f second' % (t_end - t_start))

        except StopIteration:
            count = pd.read_sql_query('select count(*) from "{}"'.format(table_name), con=engine)
            print(count)
            break


if __name__ == "__main__" :
    
    parser = argparse.ArgumentParser(description='Ingest CSV to pgSQL')

    parser.add_argument('--user', required=True, help='postgres username')
    parser.add_argument('--password', required=True, help='postgres password')
    parser.add_argument('--host', required=True, help='postgres host')
    parser.add_argument('--port', required=True, help='postgres port')
    parser.add_argument('--db', required=True, help='postgres database name')
    parser.add_argument('--table_name', required=True, help='Destination tablename for loading data to table')
    parser.add_argument('--url', required=True, help='Url to download data from as source csv')

    args = parser.parse_args()

    ingest(args)