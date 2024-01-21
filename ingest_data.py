import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #crear engine
    engine = create_engine('postgresql://'+str(user)+':'+str(password)+'@'+str(host)+':'+str(port)+'/'+str(db))

    #cargar data de taxis    
    csv_name = "output.csv"
    url_zones = "https://github.com/Cbas12/MisArchivos/raw/main/taxi_zone_lookup.csv"
    csv_zones_name = "zones.csv"

    #cargar archivo de zones
    os.system("wget "+str(url_zones)+" -O "+str(csv_zones_name))
    df_zones = pd.read_csv(csv_zones_name)
    df_zones.to_sql(name="taxi_zone_lookup",con=engine, if_exists="replace")

    #download CSV big file
    os.system("wget "+str(url)+" -O "+str(csv_name))
    

    df_iter = pd.read_csv(csv_name, iterator=True,chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,con=engine, if_exists="replace") #crear modelo de datos

    df.to_sql(name=table_name,con=engine, if_exists="append") #cargar datos

    while True:
        t_start = time() #minuto de inicio
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name,con=engine, if_exists="append") #cargar datos

        t_end = time() #minuto de fin
        
        print("inserted another chunk..., tomó " + str(t_end-t_start))




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("user",help="user name for postgres")
    parser.add_argument("password",help="password for postgres")
    parser.add_argument("host",help="host for postgres")
    parser.add_argument("post",help="port for postgres")
    parser.add_argument("db",help="database name for postgres")
    parser.add_argument("table-name",help="name of the table where we will write the results to")
    parser.add_argument("url",help="url of the csv file")

    args = parser.parse_args()
    main(args)
