'''
 # @ Author: Padma Carstens, James Tuttle. Parts of the code were taken from Jeremiah Puryear's data pipeline workflow.
 # @ University: Virginia Tech
 # @ Department: Virginia Tech University libraries (VTUL)
 # @ Project: Data Pipeline with Data Analytics team
 # @ Team: Data Services
 # @ License: MIT License
 # @ Create Time: 2023-11-28 17:43:59
 '''
'''
Purpose:
The following lambda function gets triggered when libinsight data gets updated on a weekly basis.

'''

import boto3
# Query string to execute
#query = 'SELECT * FROM "AwsDataCatalog"."libinsightdatabase"."libinsightdata"'
query = 'SELECT * FROM AwsDataCatalog.libinsightdatabase.libinsightdata'
#query=libinsightQueryOnS3csvTable

# Database to execute the query against
DATABASE = 'libinsightdatabase'

# Output location for query results
output='s3://lib-insight-serialized-data-created-in-east1-connect-to-athena/libinsightQueryOnS3csvTable/'

def lambda_handler(event, context):
    # Initiate the Boto3 Client
    client = boto3.client('athena')

    # Start the query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output
        }
    )

    # Return response after starting the query execution
    return response