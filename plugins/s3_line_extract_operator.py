import os
from airflow.models.baseoperator import BaseOperator
from airflow.plugins_manager import AirflowPlugin
import logging
import boto3


class S3LineExtractOperator(BaseOperator):
    def __init__(
        self,
        user_region: str,
        target_bucket: str,
        target_key: str,
        line_no: int,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.user_region = user_region
        self.target_bucket = target_bucket
        self.target_key = target_key
        self.line_no = line_no

#key, bucket 

    def execute(self, context):
        try:
            lines = []
            s3 = boto3.resource(
                service_name="s3",
                region_name=self.user_region,
                # aws_access_key_id= ACCESS_ID ,
                # aws_secret_access_key= ACCESS_KEY
            )
            obj = s3.Object(self.target_bucket, self.target_key)
            body = obj.get()["Body"].read().decode("utf-8")
            body_list = body.splitlines()

            logging.info("This is the desired line from the text file: ")
            print(body_list[self.line_no - 1])
        except Exception as e:
            logging.error(e)
            logging.error("Unable to extract the line. Exiting...")
            os._exit(1)


# Defining the plugin class
class S3LineExtractOperatorPlugin(AirflowPlugin):
    name = "S3LineExtractOperator"
    operators = [S3LineExtractOperator]
