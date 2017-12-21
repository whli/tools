#-*-coding:utf8-*-

import os
import boto3
from botocore.client import Config

def upload_flie(s3_path, local_file_name, back_path, file_name):   
    """
    文件上传函数(原文件转移到别处)
    """
    ary = s3_path.strip("/").split("/")
    s3_bucket = ary[2]
    s3_file_name = "/".join(ary[3:]) + "/" + file_name
    os.system("/usr/bin/aws s3 mv %s %s --recursive" %(s3_path,back_path))

    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    s3.meta.client.upload_file(local_file_name, s3_bucket, s3_file_name)
    obj = s3.Object(s3_bucket, s3_file_name)
    obj.Acl().put(ACL='public-read')

def upload_file_rm(s3_path, local_file_name, file_name):
    """
    文件上传函数(原文件删除)
    """
    ary = s3_path.strip("/").split("/")
    s3_bucket = ary[2]
    s3_file_name = "/".join(ary[3:]) + "/" + file_name
    os.system("/usr/bin/aws s3 rm %s --recursive" %s3_path)

    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    s3.meta.client.upload_file(local_file_name, s3_bucket, s3_file_name)
    obj = s3.Object(s3_bucket, s3_file_name)
    obj.Acl().put(ACL='public-read')

def load_data(s3_path, local_file):
    """
    批量数据下载函数
    """
    # 设置临时文件路径
    cleanfile = "rm %s" %(local_file)
    os.system(cleanfile)  # 清除文件，以便下次运行
    local_path = "%s_path" %local_file

    # s3数据获取
    #loadcmd = "aws s3 sync %s %s --region ap-northeast-2" %(s3_path, locale_path) # --region 控制机器操作的区域
    loadcmd = "/usr/bin/aws s3 sync %s %s" %(s3_path, local_path)
    os.system(loadcmd)
    mergecmd = "cat %s/* > %s" %(local_path, local_file)
    os.system(mergecmd)
    cleancmd = "rm -r %s" %(local_path)
    os.system(cleancmd)

def load_file(s3_file, local_file):
    """
    单文件下载函数
    """
    command = "aws s3 cp %s %s" %(s3_file,local_file)
    os.system(command)

def move_data(tar_path, backup_path):
    """
    数据移动函数
    """
    command = "aws s3 mv %s %s --recursive" %(tar_path, backup_path)
    os.system(command)

def rm_data(tar_obj):
    """
    删除文件或目录; 若目标是目录，--recursive 可同时删除目录及文件
    """
    command = "/usr/bin/aws s3 rm %s --recursive" %tar_obj
    os.system(command)

def file_exit_detect(tar_file):
    """
    检测文件或目录是否存在；若存在则返回0，否则返回256
    """
    exit_flg = os.system("aws s3 ls %s" %tar_file)
    return exit_flg

if __name__ == "__main__":
    load_data("s3://mxvp-online-tracking-log/production/2017/11/07","./1108")
