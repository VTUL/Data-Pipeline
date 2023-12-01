# Data Pipeline workflow details:

## Setup details:

- [ ] Start an ec2 instance vtul-data-pipeline
- [ ] Create a virtual environment:
```
$mkdir data-pipeline
$cd data-pipeline
$sudo apt install python3.11
$sudo apt install python3.11-venv
$python3.11 -m venv libInsight_venv
$source ./libInsight_venv/bin/activate
```
- [ ] Install aws and connect to aws cli:
```
ubuntu@ip-177-77-77-777:~/data-pipeline$ source libInsight_venv/bin/activate
Install aws:
$pip3 install --upgrade awscli
$aws --version
```
- [ ] Create a lambda function libInsightData_ec2inst. Configure IAM role 'libInsightData_ec2inst-role-zfho2sso' for libInsightData_ec2inst with necessary policies for uploading layers to lambda from s3. Tags are: [application: DataPipeline, name: vtul-data-pipeline, owner: dataservices, stack: dev, unit: padma, user: AnalyticsAssessment]
- [ ] Configure aws credentials on [Visual Studio Code](https://code.visualstudio.com/Download):
$aws configure
$enter access key :
$enter secret access key
$enter region
$enter format
  
- [ ] Set up ssh connection to [Data Pipeline project on vt gitlab](https://code.vt.edu/dataservices/data-pipeline) for versioning

- [ ] Clone the gitlab repository:
ubuntu@ip-177-77-77-777:~/data-pipeline/libInsight_venv$ git clone git@code.vt.edu:dataservices/data-pipeline.git

- [ ] Set up environment variables using a bash script 'setEnvironVariables.sh'. Create another bash script 'setEnvironVariables-local.sh' which has the credentials filled in. Add the 'setEnvironVariables-local.sh' to the git ignore list. Run using 'source setEnvironVariables-local.sh'. These credentials set up as environment variables are then accessed by the code using os.environ.get. For eg: LibInsightHostName=os.environ.get("LibInsightHostName")

## Script details

- libInsightData_ec2inst/lambda_function.py:

This code is setup to run both on aws lambda (libInsightData_ec2inst.py) and as a python script on ec2 instance(lambda_function.py).
Following lines of code helps to simulate running this code on ec2 instance as if it was running in an aws lambda environment:
```
if __name__ == "__main__":
    event = []
    context = []
    lambda_handler(event, context)
```
- Get query grabs data from libinsight api as a json file:
```
  url = 'https://vt.libinsight.com/v1.0/custom-dataset/'+libID+'/data-grid?request_id='+requestID+'&from='+fromDate+'&to='+toDate
```

- Parameters used in get query:
 
 - [ ] libID:
LibInsight ID '28364' is used to get data from all reference prods. This includes data collected from all the library locations.
-request ID: request ID of '16' is used
 
 - [ ] date range:
For the first run the date range is from 2021/11/03-2023/11/02. Following lines of code set the date range. updateLibData is set to '0' to collect all the data from the first date of data collection until 2023/11/02. Since libinsight api only allows quering data on an yearly basis, the years are broken down in an array:
 
    if updateLibData==0:
      fromDate=["2021-11-03","2022-11-03"]
      toDate=["2022-11-02","2023-11-02"]

[ ] Obtaining date range for weekly updates:
 
After the date 2023/11/02, data is collected in weekly increments from [start date of the week] to [end date of the week] for 'updateLibData' not equal to '0'. [start date of the week] is obtained by reading the dates on the previous data file and finding the maximum date:
``` 
Get the previous data file stored in the s3 bucket:
      csv_obj = s3.get_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv')
Convert csv file to pandas dataframe:
      current_data = csv_obj['Body'].read().decode('utf-8')
      existingrecords_df = pd.read_csv(StringIO(current_data))
Get all the dates from the start date column:
      startDatesdf=pd.to_datetime(existingrecords_df['_start_date'])#,dtype=object)
Get the max date time stamp from this column in the format %Y-%m-%d %H:%M:%S:
      maxDateTime=str(startDatesdf.max())
Convert the maxDateTime format to %Y-%m-%d in order to use it as [start date of the week] for the query:
      maxDateTimestrip=datetime.strptime(maxDateTime,"%Y-%m-%d %H:%M:%S")
      maxDatestr=maxDateTimestrip.strftime("%Y-%m-%d")
In order to prevent duplication of data for the max date, [start date of the week] is used as maxDatestr+1:
      maxDatestrPlus1=datetime.strptime(maxDatestr,"%Y-%m-%d")+timedelta(days=1)
      fromDate=[maxDatestrPlus1.strftime("%Y-%m-%d")]
```
[end date of the week] is taken as the current date of the week. This date is when the query is triggered to collect data on a weekly basis:
```
      toDate=[date.today().strftime('%Y-%m-%d')]
```
- [ ] libtoken:
 
This is obtained using oauth2:
``` 
  auth = HTTPBasicAuth(LibInsightClientID, LibInsightClientSecret)
  client = BackendApplicationClient(client_id=LibInsightClientID)
  oauth = OAuth2Session(client=client)
  libToken = oauth.fetch_token(token_url=LibInsightTokenURL,auth=auth)
``` 
- Append libinsight pages from the query response:
 Since linInsights api collects data one page at a time, append data from all the pages:
 
```
  libData_allpages = []
#Get the total number of pages from the response query:
  pages=response_data['payload']['total_pages']
#Append pages of data together to the empty array created above:
  for page in range(response_data['payload']['total_pages']):
     urlpage=page+1
     url = 'https://vt.libinsight.com/v1.0/custom-dataset/'+libID+'/data-grid?request_id='+requestID+'&from='+fromDate+'&to='+toDate+'&page='+str(urlpage)
     response_perpage = requests.request("GET", url, headers=headers, data=payload)
     libDataPerPage=response_perpage.json()
     libData_allpages.append(libDataPerPage)
     if urlpage == 1:
      records=libDataPerPage['payload']['records']
     else:
      newrecords=libDataPerPage['payload']['records']
      records.extend(newrecords)    
```
- Transform the query response parameters:
[ ] From the query list, delete the following parameters from the deletion list:
```
  DeletionList=["_entered_by","walkins","scans","reformat"]
  for i in range(len(records)):
    for j in range(len(DeletionList)):
      del records[i][DeletionList[j]]
```
[ ] Convert the query response data dictionary to a dataframe using pandas:
```
  cleanLibDF=pd.DataFrame(cleanLibRecords)
```
[ ] The response for the parameter 'Question Type' is returned inside brackets for eg: 'question_type: [Reference]'. Remove brackets from this column :
```
cleanLibDF['Question Type']=cleanLibDF['Question Type'].str.get(0)
```
[ ] Change date format from y-m-d h-m-s to m/d/y h-m:
```
  cleanLibDF['_start_date']=pd.to_datetime(cleanLibDF['_start_date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%m/%d/%y %H:%M')
```
 
[ ] Combine 'research' and 'topic' columns:
```
  cleanLibDF['Research_Topic'] = [''.join(i) for i in zip(cleanLibDF['research'], cleanLibDF['topic'])]
  cleanLibDF=cleanLibDF.drop(columns=['research','topic'])
``` 
- Serialize data to s3: 
 
[ ] Serialize dataframe to s3 in the memory and save it as a csv file:
``` 
  mem_file = io.BytesIO()
  cleanlibDF.to_csv(mem_file, encoding='utf-8',index=False)
``` 
 
[ ] Upload csv to s3 :
``` 
  s3 = boto3.client('s3')
  buckets = s3.list_buckets()
``` 
If updateLibData is '0' create a new record until the end date
```
  if updateLibData ==0:
    s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv',Body=mem_file.getvalue())
``` 
If updateLibData is '1' update the existing record on weekly trigger:
``` 
    # append new records to existing records
    appended_data = pd.concat([current_df, cleanlibDF], ignore_index=True)
    appended_data_encoded = appended_data.to_csv(None, index=False).encode('utf-8')
    # write the appended data to s3 bucket
    s3.put_object(Bucket='analytics-datapipeline',Key='libinsightdata-athena/LibInsightQueryData.csv',Body=appended_data_encoded)
```
