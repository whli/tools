#-*-coding:utf8-*-
"""
user:wangyue
date:20170106
"""
import boto3
from botocore.client import Config
class S3Tool(object):

    def __init__(self, local_file_name, s3_file_name, s3_bucket):
        self.local_file_name = local_file_name
        self.s3_file_name = s3_file_name
        self.s3_bucket = s3_bucket

    def __del__(self):
        pass
    
    def upload_flie(self):   
        """
        文件上传函数
        """
        s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        s3.meta.client.upload_file(self.local_file_name, self.s3_bucket, self.s3_file_name)
        obj = s3.Object(self.s3_bucket, self.s3_file_name)
        obj.Acl().put(ACL='public-read')
if __name__ == "__main__":
    s3_file_name ="loggy/20170326/"
    out_doc_file = "./1.txt"
    s3_tool_obj = S3Tool(out_doc_file, s3_file_name,'mx-hadoop')
    s3_tool_obj.upload_flie()
