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

    # Extracting file name from url
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading : {file_name} ... ')

    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    # Init sql engine
    engine = create_engine(f'postgressql://{user}:{password}@{host}:{port}/{db}')

    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)

        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)

    else:
        print('ERROR: Only CSV and PARQUET file formats allowed !')
        sys.exit()



    #create table schema, if exists : replace
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    #Load Values
    t_start = time()
    count = 0
    for batch in df_iter:
            
            count+=1
            
            if '.parquet' in file_name:
                batch_df = batch.to_pandas()
            else:
                batch_df = batch
                # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            print(f'Inserting batch: {count} ...')

            b_start = time()
            batch_df.to_sql(name=table_name, con=engine, if_exists='append')
            b_end = time()

            print(f'Batch {count} Inserted! Time taken : {b_end-b_start:10.3f} seconds.\n')

    t_end = time()
    print(f'Ingestion Completed ! took {t_end-t_start:10.3f} seconds for {count} batches.')


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