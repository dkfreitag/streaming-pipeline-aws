# Streaming Pipeline - AWS
## Streaming Data Ingestion

**Created by:** David Freitag

**AI Usage:** I used DeepSeek V4 Pro to help me write the code for this project. All architecture/system design is my own. 

### What this project is:
This repo contains a CloudFormation Stack that spins up the following:

- An HTTP API (**API Gateway**) with bearer-token auth (**Lambda function**) that accepts JSON records via POST requests
- A second Lambda function that handles records received from the API Gateway and sends them to a **Kinesis Data Stream**
  - In the event a record is malformed, the record is pushed to an **SQS queue**, and when 5 malformed records are enountered, a **CloudWatch alarm** triggers an **SNS alert** that sends you an email
- An **Amazon Data Firehose** reads from the Kinesis Data Stream and pushes records into an **S3 bucket** in batches of newline delimited JSON
- A **Glue job written in PySpark/SparkSQL** converts the raw JSON records in S3 to **Parquet** and creates an **Apache Iceberg table** in the **Glue Data Catalog**


### Architecture Diagram
```mermaid
flowchart LR
    subgraph Ingest
        Client["Client"] -->|"POST /records\nBearer token"| APIGW["API Gateway\n(HTTP API)"]
        APIGW -->|"authorize"| AuthFn["Authorizer\nLambda"]
        APIGW -->|"forward"| ProcessFn["Process\nLambda"]
    end

    subgraph Stream
        ProcessFn -->|"put_record"| KDS["Kinesis\nData Stream"]
        ProcessFn -->|"JSON errors"| ErrorQ["SQS Error\nQueue"]
        KDS -->|"poll"| Firehose["Kinesis\nData Firehose"]
    end

    subgraph Data Lake
        Firehose --> S3["S3 Bucket"]
        GlueJob["Glue PySpark\nJob"] -->|"read JSON"| S3
        GlueJob -->|"write Iceberg\nparquet/ prefix"| S3
        GlueJob -->|"register table"| Catalog["Glue Data\nCatalog\niceberg_db.iceberg_table"]
    end

    subgraph Ops
        ErrorQ -->|"metric"| Alarm["CloudWatch\nAlarm\n≥ 5 msgs"]
        Alarm -->|"notify"| SNS["SNS Topic"]
        SNS -->|"email"| Email["Email\nSubscriber"]
    end
```

### Setup

**Deploy the stack:**
```
cdk deploy --parameters AlarmEmail=<your_email@domain.com> --require-approval never
```

**Subscribe to Alerting:**

To get alerts via email when more than 5 bad records have been submitted to the API endpoint, you will need to do the following:

- When you deploy the stack with the `AlarmEmail` set to your email, you'll get an email asking you to confirm your subscription.
- Do not click the link in the email. If you click it, it will pre-fetch the URL and that will unsubscribe you automatically. More info: [https://www.reddit.com/r/aws/comments/127qim7/unexpected_automatic_unsubscribe_from_cloudwatch/](https://www.reddit.com/r/aws/comments/127qim7/unexpected_automatic_unsubscribe_from_cloudwatch/)
- Instead, examine the URL in the email to find your token.
- Run this command to confirm your subscription:

```
aws sns confirm-subscription \
    --topic-arn <arn_goes_here> \
    --token <token_goes_here>
```

### Example requests to push records to the endpoint
- The `authorizer.py` Lambda function checks the token. It must be `mysecrettokengoeshere` or the POST request will be denied.

**Unauthorized request - returns "Unauthorized"**
```
curl -X POST https://<api_url>/records \
  -H "Content-Type: application/json" \
  -d '{"_request_id": "uniquerequest1"}'
```

**Trying to authorize with the wrong token - returns "Forbidden"**
```
curl -X POST https://<api_url>/records \
  -H "Authorization: Bearer badtoken" \
  -H "Content-Type: application/json" \
  -d '{"_request_id": "uniquerequest1"}'
```

**Authorized request, returns "Invalid JSON body"**
- Record goes to the error queue
- When 5 errors have been encountered, an email alert fires
```
curl -X POST https://<api_url>/records \
  -H "Authorization: Bearer mysecrettokengoeshere" \
  -H "Content-Type: application/json" \
  -d 'not_a_json_record'
```

**Authorized request, succeeds**
```
curl -X POST https://<api_url>/records \
  -H "Authorization: Bearer mysecrettokengoeshere" \
  -H "Content-Type: application/json" \
  -d '{"_request_id": "uniquerequest1"}'
```

### Transform the submitted JSON into Parquet and create an Apache Iceberg table in the Glue Data Catalog
**Trigger AWS glue job:**
```
aws glue start-job-run --job-name json-to-parquet
```

### Viewing the error queue
**See messages in the error queue:**
```
aws sqs receive-message \
  --queue-url https://sqs.<region>.amazonaws.com/<account_id>/<queue_name> \
  --max-number-of-messages 10 \
  --visibility-timeout 30 \
  --wait-time-seconds 5
```
