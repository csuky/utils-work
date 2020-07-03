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
    # producer = KafkaProducer(bootstrap_servers=config.HOST)
    # topic = config.TOPIC

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
        dict_message = dict(zip(key, value))
        json_message = json.dumps(dict_message).replace(" ", "") + '\n'
        # InnerKafkaMessage.producer.send(InnerKafkaMessage.topic, bytes(str(json_message), 'utf-8'))
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
        driving_key = self.ecu_key
        # 车速、里程、档位数据
        driving_var = ['carSpeed', 'drivingMileage', 'gearbox']
        car_speed = random.uniform(20, 120)
        driving_mileage = ts / 10000000000
        gearbox = random.randint(0, 2)
        driving_val = [car_speed, driving_mileage, gearbox]

        for k in range(len(driving_var)):
            driving_value = ecu_com_info + [driving_var[k], driving_val[k], str(k), str(ts), '1']
            driving_message = InnerKafkaMessage.send_kafka_message(driving_key, driving_value, save_file)
        return driving_message

    def generate_ecu_data(self, var_attr_data):
        """
        根据每个异常事件的变量属性表，生成不同rc下的满足物理范围的随机数据
        :param var_attr_data: 变量属性表
        :return: 返回不同rc下的满足物理范围的随机数据
        """
        var_attr = function_attribute
        ecu_data_columns = list(range(self.record_number))
        stype_value = self.stype_prefix + var_attr_data[var_attr[0]]
        ecu_data_indexes = stype_value.tolist()
        ecu_data = pd.DataFrame(index=ecu_data_indexes, columns=ecu_data_columns)  # 索引为云端变量名，列名为rc

        # 设置不同rc值下的value
        for j in ecu_data_columns:
            ecu_data[ecu_data_columns[j]] = var_attr_data.apply(InnerKafkaMessage.generate_random_value,
                                                                args=(var_attr[1],
                                                                      var_attr[2],
                                                                      var_attr[3]),
                                                                axis=1).tolist()
        return ecu_data
    
    def generate_message_value(self, car_info, variable_info, save_file):
        """
        根据车辆信息、rc、信号数据、时间戳生成kafka的json消息，并发送到对应topic，同时保存到输出文件
        :param car_info: 车辆基本信息
        :param variable_info: 变量属性
        :param save_file: 保存文件名
        :return: 返回一个异常事件的缓存数据
        """
        event_key = self.ecu_key

        vin = car_info[self.vin]
        oem = car_info[self.oem]
        brand = car_info[self.brand]
        car_type = car_info[self.car_type]
        version = car_info[self.version]
        gps_info = car_info[config.GPS_KEY]
        
        car_com_info = [vin, oem, brand, car_type]
        ecu_com_info = car_com_info + [version]
        ts = round(time.time() * 1000)

        # 根据变量范围生成随机ECU数据
        ecu_data = self.generate_ecu_data(variable_info)

        # 生成一次事件的全部数据（字典列表）
        counter = 0

        try:
            for col, var in ecu_data.iteritems():
                self.send_gps_message(car_com_info, gps_info, ts, save_file)
                self.send_driving_message(ecu_com_info, ts, save_file)
                stype_list = list(var.index)
                for st in range(len(stype_list)):
                    event_value = ecu_com_info + [stype_list[st], str(var[stype_list[st]]), str(col), str(ts)]
                    InnerKafkaMessage.send_kafka_message(event_key, event_value, save_file)
                    counter += 1
                ts = ts + 100
        except KafkaError as e:
            print(e)
        finally:
            print(str(datetime.datetime.now())+" "+">"*20+" 共发送 %d 条<信号数：%d, 最大rc：%d>数据" % (counter, st+1, col))


if __name__ == "__main__":
    """文件输入输出"""
    file_path = config.FILE_PATH
    variable_input_file = config.VARIABLE_FILE
    test_output_file = config.OUTPUT_FILE

    """测试车辆信息"""
    car_information = config.CAR_INFO
    car_num = 1  # len(car_information)  # 车辆数量

    """功能信息"""
    function_list = config.FUNCTION_ZH
    function_attribute = config.VARIABLE_ATTRIBUTE
    variable_prefix = config.VARIABLE_PREFIX
    max_rolling = config.MAX_RECORD
    function_num = len(function_list)  # 功能列表

    io = pd.io.excel.ExcelFile(file_path + variable_input_file)
    file_name = file_path + test_output_file
    event_number = 3  # 事件次数
    wait_time = 0  # 间隔时间
    with open(file_name, 'w') as test_data_file:
        for n in range(car_num):  # 车辆数量
            for i in range(function_num):  # 功能列表
                print(str(datetime.datetime.now()) + " " + ">" * 20 + " 开始发送%s数据" % function_list[i])
                for num in range(event_number):  # 事件次数
                    raw_data = pd.read_excel(io, sheet_name=function_list[i], skiprows=1)
                    kafka_message_instance = InnerKafkaMessage(max_rolling[i], raw_data.shape[0], variable_prefix[i])
                    kafka_message_instance.generate_message_value(car_information[n], raw_data, test_data_file)
                    print(str(datetime.datetime.now())+" "+">"*20+" 第 %d 次%s发送完毕" % (num+1, function_list[i]))
                    time.sleep(wait_time)
                print()



