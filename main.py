
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import json
import os
import base64
import pyodbc
import numpy as np
import pandas as pd
from pandas import DataFrame, json_normalize
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
            ("pressure", float),
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
    meas_df['time'] = blob_df.loc[:, "EnqueuedTimeUtc"].iloc[0]
    print(meas_df)

    iterator += 1

    # This break is for debugging only, to only get first N blobs.
    if iterator > 10:
        break
