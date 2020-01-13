# -*- coding: utf-8 -*-
"""
Created on Tue Dec  21 15:36:27 2019

@author: yang.kang
@desc: Kafka消费数据
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_HOST_DEV = ["101.132.162.30:9093", "47.100.201.156:9093"] # 开发环境Kafka集群
KAFKA_HOST_PRD = ["120.131.1.153:6667","120.92.18.198:6667","120.131.0.37:6667"] # 生成环境Kafka集群公网IP
KAFKA_HOST = KAFKA_HOST_PRD
KAFKA_TOPIC = "GWMCANDATA"

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers = KAFKA_HOST)

try:
    for message in consumer:
        print(message.value)
except KafkaError as e:
    print(e)