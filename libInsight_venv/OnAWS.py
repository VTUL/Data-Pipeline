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

def lambda_handler(event, context):
    #Following are the events for Lambda
    libID="28364"
    requestID="16"
    fromDate="2023-08-30"
    toDate="023-08-31"
    libCreds=getLibCreds()
    libToken=getToken(libCreds)
    libRes=LibInQuery(libToken,libID,requestID,fromDate,toDate)
    cleanLibInData=modifyLibQueryRes(libRes)
    LibExcelFile=libInExcelToS3(cleanLibInData)
    return {
       'statusCode': 200,
       'body':json.dumps('Excel file created')
    }

#-----------------PART 1:a. get variables from aws configurations:

def getLibCreds():
    #from os import environ
    #import envVar
    LibInsightClientID=os.environ.get("LibInsightClientID")
    LibInsightClientSecret=os.environ.get("LibInsightClientSecret")
    LibInsightHostName=os.environ.get("LibInsightHostName")
    LibInsightTokenURL=os.environ.get("LibInsightTokenURL")
    LibInsightGridURL=os.environ.get("LibInsightGridURL")
   # LibInsightClientID=os.environ("LibInsightClientID")
  #  print("env 1 is ",LibInsightClientID)
  #  LibInsightClientSecret=os.environ("LibInsightClientSecret")
  #  LibInsightHostName=os.environ("LibInsightHostName")
  #  LibInsightTokenURL=os.environ("LibInsightTokenURL")
  #  LibInsightGridURL=os.environ("LibInsightGridURL")  
    LibCreds=[LibInsightClientID,LibInsightClientSecret,LibInsightHostName,LibInsightTokenURL,LibInsightGridURL]
    return LibCreds
#---------------PART 1:b. Get libInsight Token:
#x=getLibCreds()
#print("env 1 is ",x)
def getToken(libCreds):
  #config=configparser.ConfigParser()
  #config.read('configurations.ini')
  LibInsightClientID=libCreds[0]#config["LibInsightEnvVars"]["LibInsightClientID"]
  LibInsightClientSecret=libCreds[1]#config["LibInsightEnvVars"]["LibInsightClientSecret"]
  LibInsightHostName=libCreds[2]#config["LibInsightEnvVars"]["LibInsightHostName"]
  LibInsightTokenURL=libCreds[3]#config["LibInsightEnvVars"]["LibInsightTokenURL"]
  LibInsightGridURL=libCreds[4]#config["LibInsightEnvVars"]["LibInsightGridURL"]
  auth = HTTPBasicAuth(LibInsightClientID, LibInsightClientSecret)
  client = BackendApplicationClient(client_id=LibInsightClientID)
  oauth = OAuth2Session(client=client)
  token = oauth.fetch_token(token_url=LibInsightTokenURL,auth=auth)
  return token

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

#---------------------PART3: modify the list:
def modifyLibQueryRes(libRes):
#get query data as a string

  response=libRes
  jsonDict=response.json()#return json as a dictionary
  jsonResString=response.text#return json as a string
  #Delete the following parameters from the json dictionary
  DeletionList=["_entered_by","who","answeredBy","classroom","topic","walkins","employee","studio","affiliation"]

  for i in range(len(jsonDict["payload"]["records"])):
    for j in range(len(DeletionList)):
      del jsonDict["payload"]["records"][i][DeletionList[j]]

# Cleaned json lib insight query after deletion of the above parameters
  jsonDictClean=jsonDict["payload"]["records"]
  return jsonDictClean

#----------------------------PART 4: Convert the clean dictionary to a dataframe and store it in excel sheet
def libInExcelToS3(jsonDictClean):
  jsonDframe=pd.DataFrame(jsonDictClean)
  df_just_models = jsonDframe#pd.DataFrame(just_models)
  mem_file = io.BytesIO()
  #writer=pd.ExcelWriter(mem_file,engine='xlsxwriter')
  
  df_just_models.to_excel(mem_file, engine='xlsxwriter',index=False)
  
#-------------------------PART5: Upload excel sheet to s3:
  #s3 = boto3.resource('s3')
  s3 = boto3.client('s3')
  #bucket-s3.Bucket('myawsbucket751')
  #buckets = s3.list_buckets()
  #for bucket in buckets['Buckets']:
  #  print(bucket['CreationDate'].ctime(), bucket['Name'])
#Upload json string to an s3 object: 
  s3.put_object(Bucket='lib-insight-serialized-data',Key='testQueryData4.xls',Body=mem_file.getvalue())
  #s3.put_object(Bucket=bucket,Key='testQueryData4.xls',Body=mem_file.getvalue())
  #s3.put_object(Bucket='myawsbucket751',Key='testQueryData4.xls',Body=mem_file.getvalue())
  return mem_file.getvalue()

#--------------Test run 
#libID="28364"
#requestID="16"
#fromDate="2023-08-30"
#toDate="023-08-31"
#libCreds=getLibCreds()
#libToken=getToken(libCreds)
#libResp=LibInQuery(libToken,libID,requestID,fromDate,toDate)
#cleanLibInData=modifyLibQueryRes(libResp)
#LibExcelFile=libInExcelToS3(cleanLibInData)

#----------------------