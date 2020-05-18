# -*- coding: utf-8 -*-
"""
Created on Tue Dec  21 15:36:27 2019

@author: yang.kang
@desc: 固定时间戳发送数据
"""

import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_HOST = ["106.14.137.89:9093", "47.100.207.222:9093", "47.100.201.156:9093"]
KAFKA_TOPIC = "GWMCANDATA"
dataFileName = r"D:\KY\Code\Utils\utils-work\kafka\kafka_12.13.txt"
SLEEP_TIME = 30 #发送数据等待时间
testDataFileName = r"D:\KY\Code\Utils\utils-work\kafka\testDataFile.txt"

with open(dataFileName, 'r') as inputFile:
    testData = inputFile.readlines()

#VIN码、GPS、起始时间
VIN = 'LGWEF7ATEST000002'
LAT = 'lat'
LON = 'lon'
GPS_LAT = 39.9088230 #纬度 北京市
GPS_LON = 116.3974700 #经度
GPS = {LAT:str(GPS_LAT), LON:str(GPS_LON)}
START_TIME = 1577456820000 #2019-12-27 22:27:00

#时间戳生成
lenData = len(testData)
stepTime = 30000 #时间步长
stopTime = START_TIME + (lenData - 1) * stepTime
tsNew = range(START_TIME, stopTime+stepTime, stepTime)

testDataFile = open(testDataFileName, 'w')

# Kafka生产者，发送数据
producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
try:
    for i in range(lenData):
        temp = json.loads(testData[i].rstrip('\n'))
        temp['vin'] = VIN
        temp['ts'] = str(tsNew[i])
        temp['gps'] = GPS
        temp = json.dumps(temp).replace(" ", "")  + '\n'
        producer.send(KAFKA_TOPIC, bytes(str(temp), 'utf-8'))
        testDataFile.write(temp)
        time.sleep(SLEEP_TIME)
except KafkaError as e:
    print(e)
finally:
    producer.close()
    print('___Send Over___')
    testDataFile.close()
