# Data Pipeline Project

The data pipeline project was created for data analytics team at [data services](https://lib.vt.edu/research-teaching/data-services.html) in VT university libraries. The project collects data on a weekly basis from [libinsight](https://vt.libinsight.com/) to an [aws s3 bucket](https://s3.console.aws.amazon.com/). The data is queried by [aws athena](https://aws.amazon.com/athena/) and uploaded to [Tableau](https://www.tableau.com/)

The script(lambda function) is broken down into the following parts:

-Get query results from [libinsight api](https://ask.springshare.com/libinsight/faq/2100) using libinsight ID, data range and libinsight token
-Append libinsight query results returned one page at a time togethere as one dictionary
-Transform the query response parameters
-Serialize the data to s3 bucket and upload the record as a csv file

The lambda function is triggered on a weekly basis. The new records are appended to the existing records in the S3 bucket with the weekly triggers. 
Athena query is triggered on a weekly basis upon s3 bucket updates. Athena query results are then fed to Tableau for analyzing data.  

## Documentation

See the [wiki](https://code.vt.edu/dataservices/data-pipeline/-/wikis/home) for documentation. For more detailed documentation see []

## Environment

This project is hosted in the [Data Services AWS account](https://260377936955.signin.aws.amazon.com/console).  AWS components are identified by the following tags:
- Unit : AnalyticsAssessment
- Owner : DataServices
- Stack : Dev
- User :  Padma
- Application : DataPipeline



## Getting started

For installation documentation see [readme-notes.md](https://code.vt.edu/dataservices/data-pipeline/-/blob/main/readme-notes.md).
