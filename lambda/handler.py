import hashlib
import json
import os

import boto3

kinesis_client = boto3.client("kinesis")
sqs_client = boto3.client("sqs")

STREAM_NAME = os.environ.get("STREAM_NAME", "")
ERROR_QUEUE_URL = os.environ.get("ERROR_QUEUE_URL", "")


def handler(event, context):
    body = event.get("body")
    if body is None:
        print(json.dumps({"error": "Missing request body", "event": event}))
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing request body"}),
        }

    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            print(json.dumps({"error": "Invalid JSON body", "raw_body": body}))
            sqs_client.send_message(
                QueueUrl=ERROR_QUEUE_URL,
                MessageBody=json.dumps({"raw_body": body}, default=str),
            )
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Invalid JSON body"}),
            }

    records = body if isinstance(body, list) else [body]

    shard_ids = []
    for record in records:
        try:
            response = kinesis_client.put_record(
                StreamName=STREAM_NAME,
                # Firehose concatenates records without delimiters; trailing
                # newline ensures each record lands on its own line in S3
                Data=json.dumps(record) + "\n",
                # sort_keys ensures consistent JSON serialization; md5 produces
                # a stable hash so identical records always route to the same shard
                PartitionKey=hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest(),
            )
            shard_ids.append(response["ShardId"])
        except Exception as e:
            print(json.dumps({"error": "put_record failed", "record": record, "exception": str(e)}))

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"shard_ids": shard_ids}),
    }
