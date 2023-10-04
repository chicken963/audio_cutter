import json
import logging
from datetime import datetime

import yaml

from os import remove

from pydub import AudioSegment
from kafka_utils import *
from s3_utils import *

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] - %(name)s - %(message)s',
    filename=f"console_{datetime.today().strftime('%Y-%m-%d')}.log",
    encoding='utf-8',
    level=logging.INFO)


def read_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)


config_map = read_config()


def cut_local_audiofile(input_file, output_file, start_time, end_time):
    audio = AudioSegment.from_file(input_file)
    trimmed_audio = audio[start_time:end_time]
    trimmed_audio.export(output_file, format="mp3")
    logging.info(f"File {input_file} is successfully cut to {output_file}")


def trim_remote_audiofile(file_s3_key, start_time, end_time):
    uncut_file_name_local = extract_file_name_from_s3_key(file_s3_key)
    cut_file_name_local = compose_cut_file_name(uncut_file_name_local, start_time, end_time)
    cut_file_s3_key = generate_s3_key(file_s3_key, cut_file_name_local)

    download_from_s3(file_s3_key, uncut_file_name_local)
    cut_local_audiofile(uncut_file_name_local, cut_file_name_local, start_time, end_time)
    upload_to_s3(cut_file_name_local, cut_file_s3_key)

    remove(uncut_file_name_local)
    remove(cut_file_name_local)
    logging.info(f"Removed local copies of {uncut_file_name_local} and {cut_file_name_local}")
    return cut_file_name_local


def main():
    kafka_output_topic = config_map['kafka']['topic-name']['output']

    kafka_consumer = prepare_consumer()
    kafka_producer = prepare_producer()

    for message in kafka_consumer:
        message_body = json.loads(message.value)

        file_s3_key = message_body['s3_key']
        start_time = message_body['start_time']
        end_time = message_body['end_time']

        file_name_cut = trim_remote_audiofile(file_s3_key, start_time, end_time)
        kafka_producer.send(kafka_output_topic, {
            's3_key': file_name_cut,
            'start_time': start_time,
            'end_time': end_time})


if __name__ == "__main__":
    main()
