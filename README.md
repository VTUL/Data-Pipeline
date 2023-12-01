# Data Pipeline Project

The data pipeline project was created for data analytics team at [data services](https://lib.vt.edu/research-teaching/data-services.html) in VT university libraries(VTUL). The project was designed for Ellie Kohler, the head of Library Data Analytics and Assessment team to analyze library data. The project collects data on a weekly basis from [libinsight](https://vt.libinsight.com/) to an aws s3 bucket [analytics-datapipeline](https://s3.console.aws.amazon.com/s3/buckets/analytics-datapipeline?region=us-east-1&prefix=libinsightdata-athena/&showversions=false). The gathered data is collected in csv format. The data is then queried by [aws athena](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor/history/6a79c232-b98b-4b64-88b6-642c0f439433) and uploaded to [Tableau](https://www.tableau.com/) for analysis purposes. The script that collects libinsight data: [libInsightData_ec2inst](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/libInsightData_ec2inst?tab=code) is a lambda function. It is [triggered](https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/eventbus/default/rules/run_libinisghtQuery_weekly) on a weekly basis. In the [libinsight athena database](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor), a table is created mapping the original [libinsight data file](https://s3.console.aws.amazon.com/s3/object/analytics-datapipeline?region=us-east-1&prefix=libinsightdata-athena/LibInsightQueryData.csv). The [athena query](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor/history/6a79c232-b98b-4b64-88b6-642c0f439433) then performs a query on the the original [libinsight data file](https://s3.console.aws.amazon.com/s3/object/analytics-datapipeline?region=us-east-1&prefix=libinsightdata-athena/LibInsightQueryData.csv) and stores the results in a different s3 bucket [lib-insight-serialized-data..](https://s3.console.aws.amazon.com/s3/buckets/lib-insight-serialized-data-created-in-east1-connect-to-athena?region=us-east-1&prefix=libinsightQueryOnS3csvTable/&showversions=false). This athena query is coded into a [lambda function](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/automate-libinsight-athenaQuery?tab=code) . The [trigger](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/automate-libinsight-athenaQuery?tab=configure) goes off for this [lambda function](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/automate-libinsight-athenaQuery?tab=code) everytime the original [libinsight s3 data file](https://s3.console.aws.amazon.com/s3/object/analytics-datapipeline?region=us-east-1&prefix=libinsightdata-athena/LibInsightQueryData.csv) gets updated which is on a weekly basis. 

[Tableau](https://www.tableau.com/) account is associated with the user [data-analytics-team](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/users/details/data-analytics-team?section=permissions). The [IAM policy](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/policies/details/arn%3Aaws%3Aiam%3A%3A260377936955%3Apolicy%2Fdata-analytics-tableau-to-athena-s3-accesspolicy?section=permissions) on this user provides tableau account holder(Ellie Kohler) access to athena queries(read and write), access to the original s3 bucket [analytics-datapipeline](https://s3.console.aws.amazon.com/s3/buckets/analytics-datapipeline?region=us-east-1&prefix=libinsightdata-athena/&showversions=false) (read -only access) and access to the athena query results s3 bucket [lib-insight-serialized-data..](https://s3.console.aws.amazon.com/s3/buckets/lib-insight-serialized-data-created-in-east1-connect-to-athena?region=us-east-1&prefix=libinsightQueryOnS3csvTable/&showversions=false) (read-write-access). The athena query results are uploaded to Tableau. The query results are also automated on a weekly basis based on the athena query updates

The script(lambda function) is broken down into the following parts:

-Get query results from [libinsight](https://vt.libinsight.com/) using [libinsight api](https://ask.springshare.com/libinsight/faq/2100) with parameters: libinsight ID, data range and libinsight token
-Append all the pages of the libinsight query results together as one dictionary. Libinsight api returns query results that are limited to one page at a time.
-Transform the query response parameters to fit the needs of the data analytics team.
-Serialize the data to s3 bucket and upload the record as a csv file
-Create athena query and store query results in the s3 bucket
-Upload and automate athena query results to tableau for data analysis
-Add triggers to automate the upload on a weekly basis

The lambda function to start the ec2 instance is [StartLibInsightEC2Instance](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/StartLibInsightEC2Instance?tab=code). The lambda function to stop the ec2 instance is [StopLibInsightEC2Instance](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/StopLibInsightEC2Instance?tab=code). Triggers are also added to these instances.


## Documentation

See the [wiki](https://code.vt.edu/dataservices/data-pipeline/-/wikis/home) for documentation. For more detailed documentation see readme-notes.md

## Environment

This project is hosted in the [Data Services AWS account](https://260377936955.signin.aws.amazon.com/console).  AWS components are identified by the following tags:
- Unit : AnalyticsAssessment
- Owner : DataServices
- Stack : Test
- User :  elliek
- Application : DataPipeline


