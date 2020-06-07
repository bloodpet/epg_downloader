import boto3

from .app import settings


class S3(object):
    bucket = settings.AWS_STORAGE_BUCKET_NAME
    endpoint = settings.AWS_S3_ENDPOINT_URL

    def __init__(self):
        self.session = boto3.session.Session()

    @property
    def client(self):
        return self.session.client(
            "s3",
            region_name=settings.AWS_REGION_NAME,
            endpoint_url=self.endpoint,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )

    def download(self, filename):
        remote_name = self.get_key(filename)
        self.client.download_file(
            self.bucket, remote_name, filename,
        )
        return self.client.head_object(Bucket=self.bucket, Key=remote_name)

    def get_key(self, filename):
        if settings.AWS_S3_PREFIX:
            return f"{settings.AWS_S3_PREFIX}/{filename}"
        else:
            return filename

    def upload(self, filename, extra_args=None):
        if extra_args is None:
            extra_args = {}
        extra_args["ACL"] = "public-read"
        remote_name = self.get_key(filename)
        self.client.upload_file(
            filename, self.bucket, remote_name, ExtraArgs=extra_args,
        )
        return remote_name

    def upload_content(self, filename, content):
        remote_name = self.get_key(filename)
        self.client.upload_fileobj(
            content, self.bucket, remote_name, ExtraArgs={"ACL": "public-read"},
        )
        return remote_name

    def delete(self, key):
        self.client.delete_object(Bucket=self.bucket, Key=key)
