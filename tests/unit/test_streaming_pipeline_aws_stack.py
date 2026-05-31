import aws_cdk as core
import aws_cdk.assertions as assertions
from streaming_pipeline_aws.streaming_pipeline_aws_stack import StreamingPipelineAwsStack


def test_s3_bucket_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 1)


def test_kinesis_stream_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Kinesis::Stream", 1)


def test_firehose_delivery_stream_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::KinesisFirehose::DeliveryStream", {
        "DeliveryStreamType": "KinesisStreamAsSource",
    })


def test_lambda_function_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::Lambda::Function", {
        "Handler": "handler.handler",
        "Runtime": "python3.12",
    })


def test_api_gateway_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::ApiGatewayV2::Api", 1)


def test_error_sqs_queue_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SQS::Queue", 1)


def test_glue_job_created():
    app = core.App()
    stack = StreamingPipelineAwsStack(app, "streaming-pipeline-aws")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::Glue::Job", {
        "Command": {
            "Name": "glueetl",
            "PythonVersion": "3",
        },
        "GlueVersion": "4.0",
    })
