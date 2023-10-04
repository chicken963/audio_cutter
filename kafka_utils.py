import json
import logging
from datetime import datetime

import yaml
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] - %(name)s - %(message)s',
    filename=f"console_{datetime.today().strftime('%Y-%m-%d')}.log",
    encoding='utf-8',
    level=logging.INFO)


def read_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)


config_map = read_config()
bootstrap_servers = config_map['kafka']['bootstrap-servers']


def prepare_consumer():
    kafka_input_topic = config_map['kafka']['topic-name']['input']

    consumer = KafkaConsumer(kafka_input_topic,
                             api_version=(0, 11, 5),
                             group_id=config_map['kafka']['group-name']['input'],
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest')
    consumer.subscribe([kafka_input_topic])
    return consumer


def prepare_producer():
    return KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         api_version=(0, 11, 5),
                         bootstrap_servers=bootstrap_servers)
