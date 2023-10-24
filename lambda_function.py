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

    libCreds=getLibCreds()
    libToken=getToken(libCreds)
    #print('token is ',libToken)
    #print('libCreds are ',libCreds)
    libID="28364"
  #  libID="28410"
    #libID="3224"
    requestID="16"
    #fromDate="2023-08-01"
    #toDate="023-08-30"
    fromDate="2022-10-17"
    toDate="2023-10-16" 
    fromDate=["2021-10-17","2022-10-17"]
    toDate=["2022-10-16","2023-10-16"] 
    #combine records from starting date of 12/06/2021 to 10/16/2023
    allrecords=[]
    for i in range(len(fromDate)):
      records=LibInQuery(libToken,libID,requestID,fromDate[i],toDate[i])
      allrecords.extend(records)
    records=allrecords
    #print('records are ',records)
    cleanLibRecords=modifyLibQueryRes(records)
    LibDataFile=libDataToS3(cleanLibRecords)
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
  #Make get request
  response = requests.request("GET", url, headers=headers, data=payload)
  print("response is ",response)
  response_data=response.json()
  #create list to store the values:
  libData_allpages = []
  pages=response_data['payload']['total_pages']
  for page in range(response_data['payload']['total_pages']):
     urlpage=page+1
     url = 'https://vt.libinsight.com/v1.0/custom-dataset/'+libID+'/data-grid?request_id='+requestID+'&from='+fromDate+'&to='+toDate+'&page='+str(urlpage)
     response_perpage = requests.request("GET", url, headers=headers, data=payload)
     libDataPerPage=response_perpage.json()
     if urlpage ==1: print("###################### Total number of records for dates "+fromDate+" to "+toDate+ "are: ",libDataPerPage['payload']['total_records'],"######################")
     #Get all libdata as a dictionary appended together eg: {type1:..,payload1:..,records1:[]}{type2:..,payload2:..,records2:[]}
     libData_allpages.append(libDataPerPage)
     #Get only records in libdata as a list of independent pages eg: [{'_id1': 10201, '_start_date1': '2023-08-01 12:59:00'},{'_id1': 10202, '_start_date1': '2023-08-01 12:59:00'},{'_id2': 10200, '_start_date2': '2023-08-01 12:59:00'},{'_id2': 10199, '_start_date2': '2023-08-01 10:37:00'}...] where id1.. corres. to page1 and id2.. corres. to page2     
     if urlpage == 1: 
      records=libDataPerPage['payload']['records']
     else:
      newrecords=libDataPerPage['payload']['records']
      records.extend(newrecords)        
        
  return records


#---------------------PART3: modify/transforming the list:
def modifyLibQueryRes(librecords):
#libinsight records as a list for all the pages 
  records=librecords
  DeletionList=["_entered_by","walkins","scans","reformat"]
  for i in range(len(records)):
    for j in range(len(DeletionList)):
      del records[i][DeletionList[j]]

# Cleaned lib insight query records after deletion of the above parameters
  cleanLibRecords=records
  return cleanLibRecords

#----------------------------PART 4: Convert the clean dictionary to a dataframe and store it in excel/csv 
def libDataToS3(cleanLibRecords):
  #clean lib insight records as a dataframe
  libRecsDF=pd.DataFrame(cleanLibRecords)
  #------remove brackets from question type 
  libRecsDF['Question Type']=libRecsDF['Question Type'].str.get(0)
  #------change date format from y-m-d h-m-s to m/d/y h-m
  libRecsDF['_start_date']=pd.to_datetime(libRecsDF['_start_date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%m/%d/%y %H:%M')
  #print(libRecsDF['Question Type'])
  #print(libRecsDF['_start_date'])
  #quit()
  #------combine research and topic 
  libRecsDF['Research_Topic'] = [''.join(i) for i in zip(libRecsDF['research'], libRecsDF['topic'])]
  libRecsDF=libRecsDF.drop(columns=['research','topic'])
  #serialize dataframe to s3 in the memory
  mem_file = io.BytesIO()
  #----------to excel
  #writer=pd.ExcelWriter(mem_file,engine='xlsxwriter')
  #Write libinsight data to excel file
  #libRecsDF.to_excel(mem_file, engine='xlsxwriter',index=False)
  #Write libinsight data to csv file
  #------to csv
  libRecsDF.to_csv(mem_file, encoding='utf-8',index=False)

  
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
#--------------lambda test run 

if __name__ == "__main__":
    event = []
    context = []
    lambda_handler(event, context)
#----------------------