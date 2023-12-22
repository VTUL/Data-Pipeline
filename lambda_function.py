'''
 # @ Author: Padma Carstens, James Tuttle. Parts of the code were taken from Jeremiah Puryear's data pipeline workflow.
 # @ University: Virginia Tech
 # @ Department: Virginia Tech University libraries (VTUL)
 # @ Project: Data Pipeline with Data Analytics team
 # @ Team: Data Services
 # @ License: MIT License
 # @ Create Time: 2023-10-03 16:30:54
 '''
"""

Purpose: 
To pipeline data from libinsight to tableau for data analytics team. Parts of code were taken from Jeremiah Puryear and Jim Tuttle's original pipelining scripts. Note: 'records' in the script refer to all the libinsight records for id 28364. The script does the following:
1. Downloads libinsight records using libID, requestID, fromDate, toDate and libinsight credentials
2. Cleans/modifies the records 
3. Uploads cleaned records to s3 bucket after serialization
4. Updates records every week as triggered by lambda function. These are then queried by athena weekly trigger 

Functions used:
lambda_handler: function used to run the script on aws lambda
getLibCreds: Gets libinisight credentials stored as environment variables
getToken: Gets libinsight token using libinsight credentials
LibInQuery: Get libinsight response query using libinsight token, libinsight ID, libinsight request ID, records to fetch 'from date' and 'to date'
deleteFields: Delete some column fields in the records obtained from the query
transformFields: Apply transformations to some column fields after converting records to a dataframe using pandas
libDFToS3: Serializes cleaned libinsight records to s3 and creates/updates libinsight records

Links:
vt gitlab repo: https://code.vt.edu/dataservices/data-pipeline
libinsights api: https://ask.springshare.com/libinsight/faq/2100

"""

import boto3
from botocore.vendored import requests 
import requests
import pandas as pd
import io
import csv
import configparser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
import json
import os
from io import StringIO
import datetime
from datetime import datetime, timedelta
import csv
import numpy as np


#----------------------------lambda_handler: function used to run the script on aws lambda
def lambda_handler(event, context):
    #Following are the events for Lambda

    libCreds=getLibCreds()
    libToken=getToken(libCreds)
    #print('token is ',libToken)
    #print('libCreds are ',libCreds)
    libID="28364"
    requestID="16"

    #if updateLibData=0, then collect old records and deposit to s3; if updateLibData=1 then collect current weekly records and append new records to the old records
    updateLibData=0
    # Note: Libinsight only allows yearly access to query data. So get data every year and append records of each year
    #Initial run: If updateLibData==0 then get all the records until the last element in toDate

    #If updateLibData==1 then find the maximum date in the existing records and append current data from the max date of past records to the current date. This will be used for triggering using lambda

    #Note: For updateLibData=1: the toDate is current Date minus 1day, this will allow the data to be collected for all the entries of the previous day irrespective of time. Setting this to current date(as opposed to subtracting a day) might result in missing out on collection of entries from the current run time to the end of the day, since time stamp is removed from toDate when running the automated inquiry on lambda
    if updateLibData==0:
      fromDate=["2021-12-02","2022-12-02","2023-12-02"]#,"2022-12-02"]
      toDate=["2022-12-01","2023-12-01","2023-12-15"]#,"2023-12-12"]
    else: 
      #Get the max date from existing records, set it as fromDate, get current date as toDate
      s3 = boto3.client('s3')
      csv_obj = s3.get_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv')
      current_data = csv_obj['Body'].read().decode('utf-8')
      existingrecords_df = pd.read_csv(StringIO(current_data))
      #get the data frame of start dates from all the records
      startDatesdf=pd.to_datetime(existingrecords_df['_start_date'])
      print('MAX MAX MAX ',startDatesdf.max(),'type of max ',type(startDatesdf.max()))
      #max date time stamp in the format %Y-%m-%d %H:%M:%S
      maxDateTime=str(startDatesdf.max())
      #remove time stamp from the max date i.e. convert to format %Y-%m-%d to use as fromDate:
      maxDateTimestrip=datetime.strptime(maxDateTime,"%Y-%m-%d %H:%M:%S")
      maxDatestr=maxDateTimestrip.strftime("%Y-%m-%d")
      #Set fromDate as max date +1day, this is to avoid the same day data printed out to csv twice. 
      maxDatestrPlus1=datetime.strptime(maxDatestr,"%Y-%m-%d")+timedelta(days=1)
      fromDate=[maxDatestrPlus1.strftime("%Y-%m-%d")]
      print("existing records max date is ",fromDate)
      toDate=datetime.today()- timedelta(days=1)
      toDate=[toDate.strftime('%Y-%m-%d')]
      print("current date", toDate)


    #If records are collected for more than one year then append the records:
    records=records_allyrs(libToken,libID,requestID,fromDate,toDate)#allrecords
    #print('maximum date time in the records is ',libMaxDate)
    #print('records are ',records)
    #Delete some fields from the records:
    cleanLibRecords=deleteFields(records)
    #LibDataFile=libDataToS3(cleanLibRecords)
    #transform fields and return records in a dataframe:
    cleanlibDF=transformFields(cleanLibRecords)
    #serialize libdata to S3
    libDFToS3(cleanlibDF,updateLibData)
    return {
       'statusCode': 200,
       'body':json.dumps('Excel file created')
    }
#----------------------------------------------------------------------------------

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
#---------------------------------------------------------------------

#---------------PART 1:b. Get libInsight Token:
def getToken(libCreds):
  #print('libCreds are : ',libCreds)
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
#-------------------------------------------------------

#------------------------------------PART 2: Make the query for the given parameters:
def LibInQuery(libToken,libID,requestID,fromDate,toDate):
  url = 'https://vt.libinsight.com/v1.0/custom-dataset/'+libID+'/data-grid?request_id='+requestID+'&from='+fromDate+'&to='+toDate

  payload = {}

  headers = {
    'Authorization': 'Bearer '+libToken["access_token"],
    'Cookie': 'PHPSESSID=sonthqb0dsn6gn8c3n7vjs6cro'
  }
  #Make a request and get the response:
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
#---------------------------------------------------------------------------------------

#--------------------------------------------------PART3: delete some fields from the records:
def deleteFields(librecords):
#libinsight records as a list for all the pages 
  records=librecords
  #print('records are:',records)
  DeletionList=["_entered_by","walkins","scans","reformat"]
  for i in range(len(records)):
    for j in range(len(DeletionList)):
      del records[i][DeletionList[j]]

# Cleaned lib insight query records after deletion of the above parameters
  cleanLibRecords=records
  return cleanLibRecords
#-------------------------------------------------------------------------------------

#--------------------PART 4: Convert the cleaned lib data dictionary to a dataframe using pandas
#def libDataFrame(cleanLibRecords):
def transformFields(cleanLibRecords):
  #clean lib insight records as a dataframe
  cleanLibDF=pd.DataFrame(cleanLibRecords)
  cleanLibDF['Question Type']=cleanLibDF['Question Type'].str.get(0)
#----------------------
  #print(libRecsDF['Question Type'])
  #print(libRecsDF['_start_date'])
  #quit()
  #------combine research and topic 
  cleanLibDF['Research_Topic'] = [''.join(i) for i in zip(cleanLibDF['research'], cleanLibDF['topic'])]
  cleanLibDF=cleanLibDF.drop(columns=['research','topic'])
  #------------deleting rows with null values in the columne start date. This avoids 'Null' being displayed in tableau:

  print('Empty rows are: ',np.where(pd.isnull(cleanLibDF['_start_date'])))
  cleanLibDF.dropna(subset=['_start_date'], inplace=True)
  #replace new lines with space. This avoids 'comments' field from being written into the id field
  cleanLibDF = cleanLibDF.replace(r'\n',' ', regex=True) 

  return cleanLibDF
#-------------------------------------------------------------------------------------------------
#----------------------PART 5: Serialize dataframe in excel/csv to s3 bucket-----------------------
def libDFToS3(cleanlibDF,updateLibData):
  #serialize dataframe to s3 in the memory
  mem_file = io.BytesIO()

#--PART6: Upload csv/json to s3 after checking: updateLibData=1: creates a new libinsight record or updateLibData=0: uploads existing libinsight record:

  s3 = boto3.client('s3')
  #  s3 = boto3.client('s3')
  buckets = s3.list_buckets()

  #---------------------write libInsight data to csv file
  if updateLibData ==0: 
    cleanlibcsvOld=cleanlibDF
    #cleanlibOld=cleanlibDF 
      #serialize dataframe to s3 in the memory
    mem_file = io.BytesIO()
    #Write libinsight dataframe to csv file    
    #print("libinsight data frame is ",cleanlibDF)
    cleanlibcsvOld.to_csv(mem_file, encoding='utf-8',index=False,na_rep='NAN',quotechar='"',quoting=csv.QUOTE_NONNUMERIC) 
    #Serialize csv to S3:
    s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv',Body=mem_file.getvalue())
    #############################################################JSON:
    #Write libinsight dataframe to json file 
    #print('json file is ',cleanlibjsonOld)
    #convert dataframe to json
    from json import loads, dumps
    newMemfile=io.BytesIO()
    cleanlibcsvOld.to_json(newMemfile,orient='records', lines=True)
    #Serialize csv to S3:
    s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.json',Body=newMemfile.getvalue())
  else:
    #Append weekly current data to existing csv:
    csv_obj = s3.get_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv')# 
    old_datacsv = csv_obj['Body'].read().decode('utf-8')
    old_csv = pd.read_csv(StringIO(old_datacsv))
    newWeeklyDF=cleanlibDF
    appended_datacsv = pd.concat([old_csv, newWeeklyDF], ignore_index=True)
    mem_filecsvNew = io.BytesIO()
    appended_datacsv.to_csv(mem_filecsvNew, encoding='utf-8',index=False,na_rep='NAN',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)

    s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv',Body=mem_filecsvNew.getvalue())
    #---------------------------
    ##############20240111 json weekly appending not working
    #json_obj = s3.get_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.json')
    #old_datajson = json_obj['Body'].read().decode()
    #old_json = old_datajson#json.loads(old_datajson)
    ##print(old_json)
     ##Append weekly current data to existing json:
    #newWeeklyJson=cleanlibDF.to_json()
    #appended_datajson = {old_json,newWeeklyJson}
    #mem_filejsonNew = io.BytesIO()
    #appended_datajson.to_json(mem_filejsonNew,orient='records', lines=True)
    ### write the appended data to s3 bucket
    #s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.json',Body=appended_datajson)
#-------------------------------------------------------------------------------------------------------------------------------

#---------------------------------------------------------------------------------------------------------

#combine records for all the years or any given date range. Libinsight returns the records one year at a time and has a limit of one year data response, so combine records for all the years in the date range------------------------------------------------
def records_allyrs(libToken,libID,requestID,fromDate,toDate):
  #combine records from starting date of 12/06/2021 to 10/16/2023
  allrecords=[]
  for i in range(len(fromDate)):
    records=LibInQuery(libToken,libID,requestID,fromDate[i],toDate[i])
    allrecords.extend(records)
  return allrecords
#--------------------------------------------------------------------------------------------------------
#--------------lambda test run 

if __name__ == "__main__":
    event = []
    context = []
    lambda_handler(event, context)
#----------------------