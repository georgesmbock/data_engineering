#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
# import operator
import gzip
import shutil

import pandas as pd
from sqlalchemy import create_engine

def decompress_file(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith(".gz"):
        compressed_file = "output.csv.gz"
        os.system(f"wget {url} -O {compressed_file}")

        # decompression du fiichier
        decompress_file(compressed_file, csv_name)
    else:
        csv_name = "output"
        os.system(f"wget {url} -O {csv_name}")

    # Génèration du moteur
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)
    # Creation des tables
    df = next(df_iter)

    # Traitement des dates
    #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
    #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime, format='%Y-%m-%d %H:%M:%S')

    # Built DDL
    pd.io.sql.get_schema(df, name=table_name, con=engine)
    # Creation de colonnes
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Ingestion des enregistrements
    while True:
        try:
            time_start = time()
            df = next(df_iter)
            # Conversion en formate date
            #df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
            #df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime, format='%Y-%m-%d %H:%M:%S')
            # Insertion des enregistrements
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            time_end = time()
            print(f'Insertion d\'un autre lot, en {(time_end - time_start):.3f} seconde(s)')
        except StopIteration:
            print("Toutes les données ont été insérées.")
            break


if __name__=='__main__':
    # Parse des arguments
    parser = argparse.ArgumentParser(description='Ingestion d\'un fich csv dans postgre')
    # user
    # password
    # host
    # port
    # db_name
    # table_name
    # url
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='user password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for database')
    parser.add_argument('--url', help='url of csv file')

    args = parser.parse_args()

    main(args)
