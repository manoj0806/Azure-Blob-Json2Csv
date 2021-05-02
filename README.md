# Azure-Blob-Json2Csv
Python function which can be deployed to azure functions using blob trigger.
This python function perfomes a delta load operation.
The function takes the existing json files and replaces them with a csv file with same file name.
It also creates a config file to which all the file names and last date modified to that blobs will be stored.
You can directly clone this repo and can deploy this function to azure functions from VS Code.
