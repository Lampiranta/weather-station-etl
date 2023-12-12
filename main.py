
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import os
import base64
import pyodbc
from sqlalchemy import create_engine, event
import numpy as np
import pandas as pd
from pandas import DataFrame, json_normalize
import dateutil.parser
from datetime import datetime
import uuid


connect_str = os.environ['CONNSTR']
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = os.environ['CONTAINERNAME']
container_client=blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs() # name_starts_with="32") #example to only print logs sent in 32 minute

iterator = 0

for blob in blob_list:
    blob_client = container_client.get_blob_client(blob)
    stream_downloader = blob_client.download_blob()
    file_reader = json.loads(stream_downloader.readall())
    blob_df = pd.DataFrame(file_reader)
    # Get the string from body cell, it contains all ruuvi sensor data
    body = blob_df.loc[:, "Body"].iloc[0]
    body = base64.b64decode(body)

    payload = json.loads(body)
    body_df = json_normalize(payload)
    print(body_df.dtypes)
    if len(body_df.columns) % 12 != 0:
        # Move blob to some error folder.
        print("Blob body is not in expected form.")
        continue
    # Create an empty DataFrame with column names and data types

    dtypes = np.dtype(
        [
            ("name", str),
            ("bda", str),
            ("temperature", float),
            ("humidity", float),
            ("pressure", int),
            ("accelerometerX", float),
            ("accelerometerY", float),
            ("accelerometerZ", float),
            ("battery", int),
            ("txpower", int),
            ("moves", int),
            ("sequence", int),
        ]
    )
    meas_df = pd.DataFrame(np.empty(0, dtype=dtypes))
    values_list = body_df.values[0]
    for i in range(int(len(body_df.columns) / 12)):
        meas_df.loc[i] = values_list[12 * i:12 * i + 12]
    d = dateutil.parser.parse(blob_df.loc[:, "EnqueuedTimeUtc"].iloc[0])
    meas_df['date'] = d.strftime('%Y-%m-%d %H:%M:%S')
    meas_df = meas_df.convert_dtypes()

    DRIVER = 'ODBC Driver 18 for SQL Server'  # ODBC Driver
    connection_string = 'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'.format(
        uid=os.environ['SERVERUSERNAME'],
        password=os.environ['SERVERPASSWORD'],
        server=os.environ['SERVERNAME'],
        database=os.environ['DATABASENAME'],
        driver=DRIVER.replace(' ', '+'))

    table_name = 'home'
    engine = create_engine(connection_string)
    meas_df.to_sql(table_name, engine, index=False, if_exists='append', schema='weather')

    iterator += 1

    # This break is for debugging only, to only get first N blobs.
    if iterator > 0:
        break
