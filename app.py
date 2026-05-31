#!/usr/bin/env python3

import aws_cdk as cdk

from streaming_pipeline_aws.streaming_pipeline_aws_stack import StreamingPipelineAwsStack


app = cdk.App()
StreamingPipelineAwsStack(app, "StreamingPipelineAwsStack")

app.synth()
