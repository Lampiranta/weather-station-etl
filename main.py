
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import json
import os
import base64
import pandas as pd
from pandas import DataFrame, json_normalize
from datetime import datetime
import uuid
#
connect_str = os.environ['CONNSTR']
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = os.environ['CONTAINERNAME']
container_client=blob_service_client.get_container_client(container_name)
blob_list = container_client.list_blobs()#name_starts_with="32") #example to only print logs sent in 32 minute

for blob in blob_list:
    blob_client = container_client.get_blob_client(blob)
    streamdownloader = blob_client.download_blob()
    fileReader = json.loads(streamdownloader.readall())
    df = pd.DataFrame(fileReader)#.iloc[0]
    #print(df)

    body = df.loc[:, "Body"].iloc[0]
    body = base64.b64decode(body)
    print(body)
    payload = json.loads(body)
    print(type(payload))
    df2 = json_normalize(payload)
    print(df2)

    break

'''
Content in body
name* : string
bda* : string
temperature* : double
humidity* : double
pressure* : int
accelerometerX* : double
accelerometerY* : double
accelerometerZ* : double
battery* : int
txpower* : int
moves* : int
sequence* : int

Where * is index of result, body may contain 1 or more instances of data. 
Example: {'name1': 'tyohuone', 'bda1': 'D4:87:59:1C:C8:1', 'temperature1': 21.47, 'humidity1': 36.77, 'pressure1': 99972, 'accelerometerX1': -0.99, 'accelerometerY1': -0.21, 'accelerometerZ1': 0.08, 'battery1': 3, 'txpower1': 4, 'moves1': 199, 'sequence1': 49041, 'name2': 'makuuhuone', 'bda2': 'E3:B3:0F:3B:F8:C', 'temperature2': 21.79, 'humidity2': 36.32, 'pressure2': 100043, 'accelerometerX2': 1.01, 'accelerometerY2': 0.11, 'accelerometerZ2': 0.01, 'battery2': 2.99, 'txpower2': 4, 'moves2': 105, 'sequence2': 61087}
'''