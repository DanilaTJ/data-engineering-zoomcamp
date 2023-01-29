#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
# import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import os

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    output_name = url
    
    # read data 
    if output_name.split('.')[-1] == 'parquet':
        df = pd.read_parquet(output_name, engine='fastparquet')
    elif output_name.split('.')[-1] in ('csv', 'gz'):
        df = pd.read_csv(output_name)
    else:
        print("Unknown data-format in url")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"start processing {output_name.split('/')[-1]} file...")
    # create schema of table
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # upload data to table
    chunk_size = 100_000
    for i in range(0, len(df), chunk_size):
        t_start = time()
        df.loc[i:i+chunk_size-1].to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'inserted {i+chunk_size-1} of {df.shape[0]}..., it took {t_end - t_start:.2f} sec')




if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Ingest Parquet data to Posgtres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)

