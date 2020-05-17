# -*- coding: utf-8 -*-
"""
Created on Tue Dec  21 15:36:27 2019

@author: yang.kang
@desc: 获取系统当前时间戳发送数据
"""

import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_HOST_DEV = ["101.132.162.30:9093", "47.100.201.156:9093"] # 开发环境Kafka集群
KAFKA_HOST_PRD = ["120.131.1.153:6667","120.92.18.198:6667","120.131.0.37:6667"] # 生成环境Kafka集群公网IP
KAFKA_HOST = KAFKA_HOST_PRD
KAFKA_TOPIC = "GWMCANDATA"
SLEEP_TIME = 30 # 发送数据时间戳间隔(s)

originalDataFile = r"D:\KY\Code\Utils\utils-work\kafka\powerGuardOrignal.txt" # 原始数据文件
testOutDataFile = r"D:\KY\Code\Utils\utils-work\kafka\powerGuardDataFile.txt" # 测试数据备份

with open(originalDataFile, 'r') as inputFile:
    testData = inputFile.readlines()

# VIN码、GPS、起始时间
VIN = 'LGWEF7ATEST000001'
LAT = 'lat'
LON = 'lon'
GPS_LAT = 39.9088230 #纬度 北京市
GPS_LON = 116.3974700 #经度
GPS = {LAT: str(GPS_LAT), LON: str(GPS_LON)}

lenData = len(testData)
testOutData = open(testOutDataFile,'w')

# Kafka生产者，发送数据
try:
    for i in range(lenData):
        temp = json.loads(testData[i].rstrip('\n'))
        temp['vin'] = VIN
        temp['ts'] = str(round(time.time()*1000)) #获取系统当前时间
        temp['gps'] = GPS
        temp = json.dumps(temp).replace(" ", "")  + '\n'
        producer.send(KAFKA_TOPIC, bytes(str(temp), 'utf-8'))
        testOutData.write(temp)
        time.sleep(SLEEP_TIME)
except KafkaError as e:
    print(e)
finally:
    producer.close()
    testOutDataFile.close()
    print('___Send Over___')
