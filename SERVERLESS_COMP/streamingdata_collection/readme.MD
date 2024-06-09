# Ingest streaming data 

## Instruction

1. Create stream 1 in Kinese Data Stream

2. Create stream 2 in Kinese Data Stream


3. Create Lambda function as producer. 
This Producer function in Lambda will generate simulated data with fields as per required, and then simultaneously send the data to Kinese's data stream 1 and 2.

4. Create Lambda function as consumer triggered by Kinesis event - stream 1
This Consumer function in Lambda does nothing but receive data from steam 1


5. Create Kinese Data Firehose to ingest data from stream 2 into S3 storage.
The Kinese Data Firehouse will proceed the ingestion once it discovered any new data arrived in stream 2.

Others: 
- Progress of Lambda consumer and producer including the output at runtime are monitored and recorded in log files of CloudWatch.
- Logs of other components can also be set up in CloudWatch for the best practice of monitoring.

