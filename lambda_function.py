#---------------connect everything with lambda function:
import boto3
from botocore.vendored import requests 
import requests
import pandas as pd
import io
import configparser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
import json
import os
from io import StringIO

#import time
#import csv
#import athena_from_s3
#import S3_cleanup

def lambda_handler(event, context):
    #Following are the events for Lambda
    libID="28364"
  #  libID="28410"
    #libID="3224"
    requestID="16"
    fromDate="2023-05-03"
    toDate="023-05-09" 
    libCreds=getLibCreds()
    libToken=getToken(libCreds)
    #print('token is ',libToken)
    #print('libCreds are ',libCreds)
    libRes=LibInQuery(libToken,libID,requestID,fromDate,toDate)
    #print('libRes is ',libRes)
    cleanLibInData=modifyLibQueryRes(libRes)
    LibExcelFile=libInExcelToS3(cleanLibInData)
    return {
       'statusCode': 200,
       'body':json.dumps('Excel file created')
    }

#-----------------PART 1:a. get variables from aws configurations:

def getLibCreds():
    LibInsightClientID=os.environ.get("LibInsightClientID")
#    print("TYPE IS ",type(LibInsightClientID), " VALUE IS ", LibInsightClientID)
    LibInsightClientSecret=os.environ.get("LibInsightClientSecret")
    LibInsightHostName=os.environ.get("LibInsightHostName")
    LibInsightTokenURL=os.environ.get("LibInsightTokenURL")
    LibInsightGridURL=os.environ.get("LibInsightGridURL")
    libCreds=[LibInsightClientID,LibInsightClientSecret,LibInsightHostName,LibInsightTokenURL,LibInsightGridURL]
    #print(LibCreds)
    return libCreds


#---------------PART 1:b. Get libInsight Token:
def getToken(libCreds):
  LibInsightClientID=libCreds[0]#config["LibInsightEnvVars"]["LibInsightClientID"]
  LibInsightClientSecret=libCreds[1]#config["LibInsightEnvVars"]["LibInsightClientSecret"]
  LibInsightHostName=libCreds[2]#config["LibInsightEnvVars"]["LibInsightHostName"]
  LibInsightTokenURL=libCreds[3]#config["LibInsightEnvVars"]["LibInsightTokenURL"]
  LibInsightGridURL=libCreds[4]#config["LibInsightEnvVars"]["LibInsightGridURL"]
  auth = HTTPBasicAuth(LibInsightClientID, LibInsightClientSecret)
  client = BackendApplicationClient(client_id=LibInsightClientID)
  oauth = OAuth2Session(client=client)
  libToken = oauth.fetch_token(token_url=LibInsightTokenURL,auth=auth)
  return libToken

#-----------------------PART 2: Make a query:
def LibInQuery(libToken,libID,requestID,fromDate,toDate):
  url = 'https://vt.libinsight.com/v1.0/custom-dataset/'+libID+'/data-grid?request_id='+requestID+'&from='+fromDate+'&to='+toDate

  payload = {}

  headers = {
    'Authorization': 'Bearer '+libToken["access_token"],
    'Cookie': 'PHPSESSID=sonthqb0dsn6gn8c3n7vjs6cro'
  }
  response = requests.request("GET", url, headers=headers, data=payload)
  return response


#---------------------PART3: modify/transforming the list:
def modifyLibQueryRes(libRes):
#get query data as a string

  response=libRes
  jsonDict=response.json()#return json as a dictionary
  print(type(jsonDict))

  #quit()
  jsonResString=response.text#return json as a string
  #print(jsonResString)
  #type(jsonResString)
  #quit()
  #Delete the following parameters from the json dictionary
#  DeletionList=["_entered_by","who","answeredBy","classroom","walkins","employee","studio","affiliation","scans","reformat"]
  DeletionList=["_entered_by","walkins","scans","reformat"]
  #print(jsonDict)
  for i in range(len(jsonDict["payload"]["records"])):
    for j in range(len(DeletionList)):
      del jsonDict["payload"]["records"][i][DeletionList[j]]

# Cleaned json lib insight query after deletion of the above parameters
  jsonDictClean=jsonDict["payload"]["records"]
  return jsonDictClean

#----------------------------PART 4: Convert the clean dictionary to a dataframe and store it in excel sheet
def libInExcelToS3(jsonDictClean):
  jsonDframeOriginal=pd.DataFrame(jsonDictClean)
  df=jsonDframeOriginal
  #------remove brackets from question type 
  df['Question Type']=df['Question Type'].str.get(0)
  #------change date format from y-m-d h-m-s to m/d/y h-m
  df['_start_date']=pd.to_datetime(df['_start_date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%m/%d/%y %H:%M')
  #print(df['Question Type'])
  #print(df['_start_date'])
  #quit()
  #------combine research and topic 
  df['Research_Topic'] = [''.join(i) for i in zip(df['research'], df['topic'])]
  df=df.drop(columns=['research','topic'])
  jsonDframe=df
  df_just_models = jsonDframe#pd.DataFrame(just_models)
  mem_file = io.BytesIO()
  #----------to excel
  #writer=pd.ExcelWriter(mem_file,engine='xlsxwriter')
  #Write libinsight data to excel file
  #df_just_models.to_excel(mem_file, engine='xlsxwriter',index=False)
  #Write libinsight data to csv file
  #------to csv
  df_just_models.to_csv(mem_file, encoding='utf-8',index=False)

  
#-------------------------PART5: Upload excel sheet to s3:

  s3 = boto3.client('s3')
  #  s3 = boto3.client('s3')
  buckets = s3.list_buckets()
  #for bucket in buckets['Buckets']:
  #  print(bucket['CreationDate'].ctime(), bucket['Name'])
#Upload json string to an s3 object: 

  #---------------------write libInsight data to excel file 
  #s3.put_object(Bucket='lib-insight-serialized-data-created-in-east1-connect-to-athena',Key='LibInsightQueryData.xls',Body=mem_file.getvalue())
  #---------------------write libInsight data to csv file
  #s3.put_object
  s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv',Body=mem_file.getvalue())

  return mem_file.getvalue()
#--------------Test run 

if __name__ == "__main__":
    event = []
    context = []
    lambda_handler(event, context)
#----------------------