"""
-------------------------------------------------
# @Time     : 5/16/2020 4:45 PM
# @Author   : Kang Yang
# @Email    : yang.kang@uaes.com
# @Description：
-------------------------------------------------
"""
import time
import datetime
import json
import pandas as pd
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config


class InnerKafkaMessage(object):

    """类属性：kafka生产者"""
    producer = KafkaProducer(bootstrap_servers=config.HOST)
    topic = config.TOPIC

    def __init__(self, record_number, var_num, var_prefix):
        """
        初始化对象，传入最大记录数、变量个数、变量名前缀
        :param record_number: 最大记录数
        :param var_num: 变量个数
        :param var_prefix: 变量名前缀
        """
        # 定义ECU消息格式中的key名
        self.vin = config.VIN
        self.stype = config.STYPE
        self.time_stamp = config.TIME_STAMP
        self.value = config.VALUE
        self.rolling_counter = config.ROLLING_COUNTER
        self.oem = config.OEM
        self.brand = config.BRAND
        self.car_type = config.CAR_TYPE
        self.version = config.VERSION
        self.ecu_key = [self.vin,
                        self.oem,
                        self.brand,
                        self.car_type,
                        self.version,
                        self.stype,
                        self.value,
                        self.rolling_counter,
                        self.time_stamp]

        # 定义gps消息格式key名
        self.lat = config.LAT
        self.lon = config.LON
        self.bearing = config.BEARING
        self.speed = config.SPEED
        self.gps_key = [self.vin,
                        self.oem,
                        self.brand,
                        self.car_type,
                        self.stype,
                        self.time_stamp,
                        self.lat,
                        self.lon,
                        self.bearing,
                        self.speed]

        # 初始化记录数、变量个数、变量名前缀
        self.record_number = record_number
        self.cloud_variable_num = var_num
        self.stype_prefix = var_prefix

    @staticmethod
    def send_kafka_message(key, value, save_file):
        """
        根据key、value生成json，将消息推到kafka，同时保存数据到文件
        :param key: list，消息中的key
        :param value: list，消息中的key
        :param save_file: 保存的文件
        :return: json，发送的json消息
        """
        dict_message = dict(zip(key, value))
        json_message = json.dumps(dict_message).replace(" ", "") + '\n'
        InnerKafkaMessage.producer.send(InnerKafkaMessage.topic, bytes(str(json_message), 'utf-8'))
        save_file.write(json_message)
        return json_message

    @staticmethod
    def generate_random_value(series, value_type, up_key, low_key):
        """
        根据变量类型和物理上下限，随机生成数据
        :param series: series类型，变量属性
        :param value_type: 变量类型 
        :param up_key: 上限字段
        :param low_key: 下限字段
        :return: 返回生成的随机值
        """
        if series[value_type].upper() == 'BIT':
            result = random.randint(0, 1)
        else:
            low_limit = series[low_key]
            up_limit = series[up_key]
            result = random.uniform(low_limit, up_limit)
        return result

    def send_gps_message(self, car_com_info, gps_info, ts, save_file):
        """
        生成gps的json消息，并调用发送消息的函数推到kafka中，并保存数据到文件
        :param car_com_info: dict，车辆基本信息
        :param gps_info: dict，gps基本信息
        :param ts: timestamp，时间戳
        :param save_file: 保存的文件
        :return: json，发送的gps json消息
        """
        gps_message_key = self.gps_key
        gps_message_value = car_com_info + [config.GPS_KEY,
                                            str(ts),
                                            gps_info[self.lat],
                                            gps_info[self.lon],
                                            gps_info[self.bearing],
                                            gps_info[self.speed]]
        gps_message = InnerKafkaMessage.send_kafka_message(gps_message_key, gps_message_value, save_file)
        return gps_message

    def send_driving_message(self, ecu_com_info, ts, save_file):
        """
        生成指定的轮转帧中变量json消息，并调用发送消息的函数推到kafka中，并保存数据到文件
        :param ecu_com_info: list，共用的基本消息内容
        :param ts: timestamp，时间戳
        :param save_file: 保存的文件
        :return: json，发送的driving json消息
        """
        driving_key = self.ecu_key
        # 车速、里程、档位数据
        driving_var = ['carSpeed', 'drivingMileage', 'gearbox']
        car_speed = random.uniform(20, 120)
        driving_mileage = ts / 10000000000
        global gearbox
        gearbox = random.randint(0, 2)
        driving_val = [car_speed, driving_mileage, gearbox]

        for k in range(len(driving_var)):
            driving_value = ecu_com_info + [driving_var[k], driving_val[k], str(k), str(ts), '1']
            driving_message = InnerKafkaMessage.send_kafka_message(driving_key, driving_value, save_file)
        return driving_message

    def generate_ecu_data(self, var_attr_data, attributes, data_abnormal):
        """
        根据每个异常事件的变量属性表，以及数据发送的异常模式，生成不同rc下的满足物理范围的随机数据
        :param var_attr_data: DataFrame，变量属性表
        :param attributes: list，属性字段
        :param data_abnormal: int，数据发送的异常模式
        :return: DataFrame，生成的某异常事件的一次ECU缓存数据
        """
        if data_abnormal == 0:
            ecu_data_columns = list(range(self.record_number))  # rc：正常情况，完整发送
        elif data_abnormal == 1:
            ecu_data_columns = list(range(0, self.record_number-3))  # rc：不完整，从0开始连续发送到大于20
        elif data_abnormal == 2:
            ecu_data_columns = list(range(0, 18))  # rc：不完整，从0开始连续发送到18
        elif data_abnormal == 3:
            ecu_data_columns = list(range(3, 18))  # rc：不完整，从3开始连续发送到18
        elif data_abnormal == 4:
            ecu_data_columns = list(range(3, 18))  # rc：不完整，从0开始间隔2发送，有rc=20
        elif data_abnormal == 5:
            ecu_data_columns = list(range(3, 18))  # rc：不完整，从0开始间隔3发送，无rc=20
        else:
            ecu_data_columns = list(range(self.record_number))  # rc：正常情况，完整发送
            print("未设置发送模式，默认正常发送")

        stype_value = self.stype_prefix + var_attr_data[attributes[0]]
        ecu_data_indexes = stype_value.tolist()
        ecu_data = pd.DataFrame(index=ecu_data_indexes, columns=ecu_data_columns)  # 索引为云端变量名，列名为rc

        # 设置不同rc值下的value
        for j in ecu_data_columns:
            ecu_data[j] = var_attr_data.apply(InnerKafkaMessage.generate_random_value,
                                              args=(attributes[1],
                                                    attributes[2],
                                                    attributes[3]),
                                              axis=1).tolist()
        return ecu_data
    
    def send_ecu_data(self, car_info, variable_info, save_file):
        """
        根据车辆信息、rc、信号数据、时间戳生成kafka的json消息，并发送到对应topic，同时保存到输出文件
        :param car_info: 车辆基本信息
        :param variable_info: 变量属性
        :param save_file: 保存文件名
        :return: 返回一个异常事件的缓存数据
        """

        vin = car_info[self.vin]
        oem = car_info[self.oem]
        brand = car_info[self.brand]
        car_type = car_info[self.car_type]
        version = car_info[self.version]
        gps_info = car_info[config.GPS_KEY]
        
        car_com_info = [vin, oem, brand, car_type]
        ecu_com_info = car_com_info + [version]
        ts = round(time.time() * 1000)

        # 根据变量范围生成异常事件随机ECU数据
        data_abnormal = 0  # 发送数据异常情况
        event_key = self.ecu_key
        variable_attribute = config.VARIABLE_ATTRIBUTE
        event_data = self.generate_ecu_data(variable_info, variable_attribute, data_abnormal)

        # 生成一次事件的全部数据（字典列表）
        message_number = 0  # 记录消息总条数
        try:
            for rc, event_data_series in event_data.iteritems():
                self.send_gps_message(car_com_info, gps_info, ts, save_file)
                self.send_driving_message(ecu_com_info, ts, save_file)
                if rc <= 2:
                    print(str(datetime.datetime.now())+" "+">"*20+" rollingCounter=%d gearbox=%d" % (rc, gearbox))
                stype_list = list(event_data_series.index)
                for var_index, var_name in enumerate(stype_list):
                    event_value = ecu_com_info + [var_name, str(event_data_series[var_name]), str(rc), str(ts)]
                    InnerKafkaMessage.send_kafka_message(event_key, event_value, save_file)
                    message_number += 1
                ts = ts + 100
            rc_list = event_data.columns.tolist()  # 一次事件的发送的rc值
        except KafkaError as e:
            print(e)
        finally:
            print(str(datetime.datetime.now())+" "+">"*20+" 发送消息：%d, 组数：%d, 每组信号：%d, rc序列：%s"
                  % (message_number, len(rc_list), var_index+1, str(rc_list)))


if __name__ == "__main__":
    """文件输入输出"""
    file_path = config.FILE_PATH
    variable_input_file = config.VARIABLE_FILE
    test_output_file = config.OUTPUT_FILE
    io = pd.io.excel.ExcelFile(file_path + variable_input_file)
    file_name = file_path + test_output_file

    """测试车辆信息"""
    car_information = config.CAR_INFO
    car_num = 1  # len(car_information)  # 车辆数量

    """功能信息"""
    function_list = config.FUNCTION_ZH
    variable_prefix = config.VARIABLE_PREFIX
    max_rolling = config.MAX_RECORD
    function_num = 1  # len(function_list)  # 功能列表

    event_number = 1  # 事件次数
    wait_time = 0  # 每次事件间隔时间
    
    with open(file_name, 'w') as test_data_file:
        for n in range(car_num):  # 车辆数量
            for i in range(function_num):  # 功能列表
                print(str(datetime.datetime.now()) + " " + ">" * 20 + " 开始发送%s数据" % function_list[i])
                for num in range(event_number):  # 事件次数
                    raw_data = pd.read_excel(io, sheet_name=function_list[i], skiprows=1)
                    kafka_message_instance = InnerKafkaMessage(max_rolling[i], raw_data.shape[0], variable_prefix[i])
                    kafka_message_instance.send_ecu_data(car_information[n], raw_data, test_data_file)
                    print(str(datetime.datetime.now())+" "+">"*20+" 第 %d 次%s发送完毕" % (num+1, function_list[i]))
                    time.sleep(wait_time)
                print()



