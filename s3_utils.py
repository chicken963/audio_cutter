import logging
from datetime import datetime

import boto3
import yaml

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] - %(name)s - %(message)s',
    filename=f"console_{datetime.today().strftime('%Y-%m-%d')}.log",
    encoding='utf-8',
    level=logging.INFO)


def read_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)


config_map = read_config()
bucket_name = config_map['amazon']['s3']['bucket-name']


def open_s3_session():
    access_key = config_map['amazon']['s3']['access-key-id']
    secret_key = config_map['amazon']['s3']['secret-access-key']

    return boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)


def download_from_s3(path, file_name):
    s3 = open_s3_session()
    s3.download_file(bucket_name, path, file_name)
    logging.info(f"File {file_name} is successfully downloaded")


def extract_file_name_from_s3_key(s3_key):
    file_name = s3_key.split("/")[-1]
    return file_name.removesuffix(".mp3")


def compose_cut_file_name(full_file_name, start_time, end_time):
    return full_file_name + f"_{start_time / 1000:.1f}_{end_time / 1000:.1f}"


def generate_s3_key(basic_key, file_name):
    folder_key = basic_key.rsplit('/', 1)[0]
    return folder_key + '/' + file_name


def upload_to_s3(file_name, s3_key):
    # s3 = boto3.client('s3')
    s3 = open_s3_session()
    s3.upload_file(file_name, bucket_name, s3_key)
    logging.info(f"File {file_name} is successfully uploaded (key in bucket is {s3_key})")
