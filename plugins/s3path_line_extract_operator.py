import os
from airflow.models.baseoperator import BaseOperator
from airflow.plugins_manager import AirflowPlugin
import logging
import boto3


class S3PathLineExtractOperator(BaseOperator):
    def __init__(
        self, user_region: str, target_path: str, line_no: int, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.user_region = user_region
        self.target_path = target_path
        self.line_no = line_no

    def execute(self, context):
        # splitting the path to object(bucket name) and key(the remaining path)
        f1 = self.target_path.split("://")
        f2 = f1[1].split("/", 1) 
        TARGET_BUCKET = f2[0]  # bucket_name
        TARGET_FILE = f2[1]  # path which includes subfolders if any and file name

        try:
            lines = []
            s3 = boto3.resource(
                service_name="s3",
                region_name=self.user_region,
                # aws_access_key_id= ACCESS_ID ,
                # aws_secret_access_key= ACCESS_KEY
            )
            obj = s3.Object(TARGET_BUCKET, TARGET_FILE)
            body = obj.get()["Body"].read().decode("utf-8")
            body_list = body.splitlines()

            logging.info("This is the desired line from the text file: ")
            print(body_list[self.line_no - 1])
        except Exception as e:
            logging.error(e)
            logging.error("Unable to extract the line. Exiting...")
            os._exit(1)


# Defining the plugin class
class s3PathLineExtractOperatorPlugin(AirflowPlugin):
    name = "s3PathLineExtractOperator"
    operators = [S3PathLineExtractOperator]
