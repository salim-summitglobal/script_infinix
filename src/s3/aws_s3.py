from typing import List, Optional, BinaryIO, Dict, Any
import aioboto3
from botocore.exceptions import ClientError
from config.settings import settings


class S3Helper:
    def __init__(self):
        self.session = aioboto3.Session()
        self.credentials = {
            'aws_access_key_id': settings.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': settings.AWS_SECRET_ACCESS_KEY,
            'region_name': settings.AWS_REGION
        }
        self.bucket_name = settings.AWS_BUCKET_NAME

    async def upload_file(self, file_content: BinaryIO, file_name: str, content_type: str) -> bool:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                await client.upload_fileobj(
                    Fileobj=file_content,
                    Bucket=self.bucket_name,
                    Key=file_name,
                    ExtraArgs={'ContentType': content_type}
                )
                return True
        except ClientError as e:
            return False

    async def get_public_url(self, s3_key: str, expiration: Optional[int] = None) -> Optional[str]:
        if expiration:
            async with self.session.client('s3', **self.credentials) as client:
                try:
                    return await client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': self.bucket_name, 'Key': s3_key},
                        ExpiresIn=expiration
                    )
                except ClientError as e:
                    return None
        return f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"

    async def delete_object(self, s3_key: str) -> bool:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                await client.delete_object(Bucket=self.bucket_name, Key=s3_key)
                return True
        except ClientError as e:
            return False

    async def get_object(self, s3_key: str) -> Dict[str, Any]:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                data = await client.get_object(Bucket=self.bucket_name, Key=s3_key)
                return data
        except ClientError as e:
            return {}

    async def delete_objects(self, s3_keys: List[str]) -> bool:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                await client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': [{'Key': key} for key in s3_keys]}
                )
                return True
        except ClientError as e:
            return False

    async def check_object_exists(self, s3_key: str) -> bool:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                await client.head_object(Bucket=self.bucket_name, Key=s3_key)
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            return False

    async def get_object_size(self, s3_key: str) -> Optional[int]:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                response = await client.head_object(Bucket=self.bucket_name, Key=s3_key)
                return response['ContentLength']
        except ClientError as e:
            return None

    async def list_objects(self, prefix: str = '') -> List[str]:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                paginator = client.get_paginator('list_objects_v2')
                keys = []
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    if 'Contents' in page:
                        keys.extend([obj['Key'] for obj in page['Contents']])
                return keys
        except ClientError as e:
            return []

    async def copy_object(self, source_key: str, dest_key: str) -> bool:
        try:
            async with self.session.client('s3', **self.credentials) as client:
                copy_source = {
                    'Bucket': self.bucket_name,
                    'Key': source_key
                }
                await client.copy_object(
                    CopySource=copy_source,
                    Bucket=self.bucket_name,
                    Key=dest_key
                )
                return True
        except ClientError as e:
            return False


    async def create_directory(self, path: str) -> str:
        try:
            if not path.endswith('/'):
                path += '/'

            async with self.session.client('s3', **self.credentials) as client:
                await client.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    Body=''
                )
                return f"s3://{self.bucket_name}/{path}"
        except ClientError as e:
            raise e