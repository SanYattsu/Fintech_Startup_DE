import boto3
from botocore.exceptions import ClientError

class S3Connector():
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str,
                 service_name: str = 's3', endpoint_url: str = 'https://storage.yandexcloud.net') -> None:
        session = boto3.session.Session()
        self.s3_resourse = session.resource(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            service_name=service_name,
            endpoint_url=endpoint_url,
        )

    def download_file(self, key: str, bucket: str, file_url: str) -> None:
        s3_bucket = self.s3_resourse.Bucket(bucket)
        try:
            s3_bucket.download_fileobj(key, file_url)
        except ClientError as e:
            raise Exception(f"Error downloading file from S3: {e}") 
