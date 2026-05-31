from constructs import Construct
from aws_cdk import (
    CfnOutput,
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs as sqs,
    aws_apigatewayv2 as apigw,
    aws_apigatewayv2_authorizers as apigw_authorizers,
    aws_apigatewayv2_integrations as apigw_integrations,
)


class StreamingPipelineAwsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create S3 bucket
        bucket = s3.Bucket(
            self, "RecordsBucket",
        )

        # create Kinesis Data Stream
        stream = kinesis.Stream(
            self, "RecordsStream",
            removal_policy=RemovalPolicy.DESTROY,
            stream_mode=kinesis.StreamMode.ON_DEMAND,
        )

        # create Firehose IAM Role
        # we will attach policies to this to give the firehose permissions it requires
        # to read from the Kinesis Data Stream and write to the S3 bucket
        firehose_role = iam.Role(
            self, "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        # give the firehose role the permissions it needs to write to the S3 bucket and
        # read from the Kinesis Data Stream
        bucket.grant_read_write(firehose_role)
        firehose_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetRecords",
                    "kinesis:GetShardIterator",
                ],
                resources=[stream.stream_arn],
            )
        )

        # create the Amazon (Kinesis) Data Firehose
        # this reads from the Kinesis Data Stream, batches records, and deposits them into S3
        firehose_delivery = firehose.CfnDeliveryStream(
            self, "RecordsFirehose",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=stream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="json/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=5,
                ),
            ),
        )

        # add dependencies for the firehose
        # ensure the policy is created and attached the role before creating the firehose
        firehose_delivery.node.add_dependency(
            firehose_role.node.find_child("DefaultPolicy")
        )

        # ensure the bucket is created too
        firehose_delivery.node.add_dependency(bucket)

        # create a queue that the Lambda will push records to if there is an error with that record
        error_queue = sqs.Queue(
            self, "ErrorQueue",
            visibility_timeout=Duration.seconds(300),
        )

        # create a parameter for setting the email that will get the alert when the error threshold
        # is reached
        alarm_email = CfnParameter(
            self, "AlarmEmail",
            type="String",
            description="Email address to receive error queue alerts",
        )

        # create SNS alarm topic
        # this will send an email 
        alarm_topic = sns.Topic(
            self, "AlarmTopic",
        )
        alarm_topic.add_subscription(
            sns_subs.EmailSubscription(alarm_email.value_as_string)
        )

        # create a CloudWatch alarm that fires once we get a certain number of errors
        # send a message to SNS
        error_alarm = cloudwatch.Alarm(
            self, "ErrorQueueAlarm",
            metric=error_queue.metric_approximate_number_of_messages_visible(),
            threshold=5,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        error_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # authorizer.py Lambda function
        auth_fn = _lambda.Function(
            self, "AuthFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="authorizer.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(10),
        )

        # configure the authorizer.py Lambda function as the authorizer
        authorizer = apigw_authorizers.HttpLambdaAuthorizer(
            "Authorizer",
            auth_fn,
            authorizer_name="token-authorizer",
            identity_source=["$request.header.Authorization"],
            response_types=[apigw_authorizers.HttpLambdaResponseType.SIMPLE],
        )

        # create the Lambda function
        # this pushes records to the Kinesis Data Stream
        process_fn = _lambda.Function(
            self, "ProcessRecordsFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(10),
            environment={
                "STREAM_NAME": stream.stream_name,
                "ERROR_QUEUE_URL": error_queue.queue_url,
            },
        )

        # give the Lambda function access to write to the Kinesis Data Stream
        stream.grant_write(process_fn)

        # give the Lambda function access to write to the error SQS queue
        error_queue.grant_send_messages(process_fn)

        # create the API Gateway
        api = apigw.HttpApi(
            self, "RecordsApi",
        )

        # add a route
        # all records that land on this route get pushed to the Lambda function
        api.add_routes(
            path="/records",
            methods=[apigw.HttpMethod.POST],
            authorizer=authorizer,
            integration=apigw_integrations.HttpLambdaIntegration(
                "ProcessRecordsIntegration", process_fn
            ),
        )

        # print the URL of the API gateway endpoint to the console
        CfnOutput(self, "ApiUrl", value=api.url)

        # create Glue job
        glue_script = s3_assets.Asset(
            self, "GlueScript",
            path="glue/glue_job.py",
        )

        # set up Glue IAM role
        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # give the Glue IAM role the required permissions
        bucket.grant_read_write(glue_role)
        glue_script.grant_read(glue_role)

        # set up the Glue ETL job
        glue.CfnJob(
            self, "JsonToParquetJob",
            name="json-to-parquet",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=glue_script.s3_object_url,
            ),
            default_arguments={
                "--datalake-formats": "iceberg",
                "--input_path": bucket.s3_url_for_object("json/"),
                "--output_path": bucket.s3_url_for_object("parquet/"),
                "--job-bookmark-option": "job-bookmark-disable",
            },
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
        )
