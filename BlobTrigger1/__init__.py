#####################################################################################################
########  Component : CSV REPLACER                                                          #########
########  Details   : Code to read JSON Files from blob,convert to CSV and load to Blob     #########
########  Version   : 1.0                                                                   #########
########  Date      : April 2021                                                            #########
########  Author    : Manoj Babu G R                                                        #########
#####################################################################################################

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import pandas as pd
import json
from datetime import datetime
from calendar import timegm

# Credentials-----------------------------------------------------------------------------------------------------------

# setting up connection to container which has the recieving blobs
service = BlobServiceClient(account_url="xxxxx",
                            credential="xxx")

sas_url = "xxx"
container = ContainerClient.from_container_url(sas_url)
container_name = 'input'

# setting up blob client to retrieve properties of each blob and append to dictionary
conn_str = 'xxx'

# All the Variables
list_of_all_blobs = []
list_of_blobs_Csv = []
old_file_names = []
final_list_of_files = []
delta_load_file_names = {}


# function convert datetime.datetime utc to unixlike timestamp
def timestamping(string):
    tm = string
    tm = tm[0:19]
    fmt = '%Y-%m-%d %H:%M:%S'
    return timegm(datetime.strptime(tm, fmt).utctimetuple())


# function to return file names
def get_filename(i):
    return final_list_of_files[i]


# Enumerating Container and storing all the blob names in a list
blobs_list = container.list_blobs()
for blob in blobs_list:
    list_of_all_blobs.append(blob.name)

#reading data of pre-existing files from config.txt
blob_client_reading_config = container.get_blob_client('config.txt')
dict_data_configs = blob_client_reading_config.download_blob().readall()
dict_data_configs = eval(dict_data_configs)
for key in dict_data_configs.keys():
    old_file_names.append(key)
final_list_of_files = list(set(list_of_all_blobs)-set(old_file_names))


# Converting file names to .csv extension to store them
for json_file_name in final_list_of_files:
    filename = str(json_file_name)
    (prefix, sep, suffix) = filename.rpartition('.')
    list_of_blobs_Csv.append(prefix + '.csv')

length_of_blobs = len(final_list_of_files)

# Code to iterate over json files and transform them to csv and delete existing json files
for file in range(0, length_of_blobs):
    blob_client = container.get_blob_client(get_filename(file))
    df = pd.read_json(blob_client.download_blob().readall())
    df.to_string()
    container.upload_blob(name=list_of_blobs_Csv[file], data=df.to_csv())
    # Delete multiple blobs in the container by name
    container.delete_blobs(get_filename(file))

list_of_all_blobs_Csv=[]
blobs_list = container.list_blobs()
for blob in blobs_list:
    list_of_all_blobs_Csv.append(blob.name)
    # list_of_all_blobs_Csv.remove('config.txt')

# code to write file and last modified date to config file to perform delta load
for file in list_of_all_blobs_Csv:
    blob_client_properties = BlobClient.from_connection_string(conn_str, container_name, file)
    properties = blob_client_properties.get_blob_properties()
    for value in properties.values():
        delta_load_file_names[properties['name']] = str(properties['last_modified'])
data_json_loaded = json.dumps(delta_load_file_names)
container.delete_blobs('config.txt')
container.upload_blob(name='config.txt', data=data_json_loaded)

