import boto3
from io import StringIO
from botocore.errorfactory import ClientError
import os

if os.path.exists('env.py'):
    import env


class S3Handler:

    def __init__(self):
        self.s3_session = boto3.session.Session(
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket = 'toothbrush-data-bucket'

    def save_to_s3(self, file, df):
        s3 = self.s3_session
        s3_res = s3.resource('s3')

        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        key = f'data/{file}'

        s3_res.Object(self.bucket, key).put(Body=csv_buffer.getvalue())

        return True

    def read_from_s3(self):

        s3 = self.s3_session.client('s3')

        s3_objects = []
        csv_files = []

        try:
            res = s3.list_objects(Bucket=self.bucket)
            for obj in res['Contents']:
                if obj['Key'].startswith('data'):
                    s3_objects.append(obj['Key'])

            for file in s3_objects:
                csv_file = s3.get_object(
                    Bucket=self.bucket,
                    Key=file
                )

                csv_files.append({
                    'filename': file,
                    'body': csv_file['Body']
                })

            return csv_files

        except ClientError as e:
            print(f'ClientError!: {e}')
            return False

    def delete_from_s3(self, filename):

        s3 = self.s3_session.client('s3')

        try:
            s3.delete_object(
                Bucket=self.bucket,
                Key=filename
            )
            return True
        except ClientError as e:
            print(f'Client Error: {e}')
            return False
