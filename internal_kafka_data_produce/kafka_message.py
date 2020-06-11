"""
-------------------------------------------------
# @Time     : 5/16/2020 4:45 PM
# @Author   : Kang Yang
# @Email    : yang.kang@uaes.com
# @Description：
-------------------------------------------------
"""
import time
import json
import pandas as pd
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config


class InnerKafkaMessage(object):

    """类属性：kafka生产者"""
    # producer = KafkaProducer(bootstrap_servers=config.HOST)

    """初始化对象，传入最大记录数、变量个数、变量名前缀"""
    def __init__(self, record_number, var_num, var_prefix):
        # 定义消息格式中的key名
        self.vin = config.VIN
        self.stype = config.STYPE
        self.time_stamp = config.TIME_STAMP
        self.value = config.VALUE
        self.rolling_counter = config.ROLLING_COUNTER
        self.oem = config.OEM
        self.brand = config.BRAND
        self.car_type = config.CAR_TYPE
        self.version = config.VERSION

        # 定义gps消息格式key名
        self.lat = config.LAT
        self.lon = config.LON
        self.bearing = config.BEARING
        self.speed = config.SPEED

        # 初始化记录数、变量个数、变量名前缀
        self.record_number = record_number
        self.cloud_variable_num = var_num
        self.stype_prefix = var_prefix

    """根据变量类型和物理上下限，随机生成数据，series为series类型"""
    @staticmethod
    def generate_random_value(series, value_type, unit, up_key, low_key):
        if series[value_type].upper() == 'BIT':
            result = random.randint(0, 1)
        else:
            low_limit = series[low_key]
            up_limit = series[up_key]
            result = random.uniform(low_limit, up_limit)
        return result

    """根据每个异常事件的变量属性表，生成不同rc下的满足物理范围的随机数据"""
    def generate_ecu_data(self, var_attr_data):
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
                                                                      var_attr[3],
                                                                      var_attr[4]),
                                                                axis=1).tolist()
        return ecu_data
    
    """根据车辆信息、rc、信号数据、时间戳生成kafka的json消息，并发送到对应topic，同时保存本地"""
    def generate_message_value(self, car_info, data_info, save_file):
        key = [self.vin,
               self.oem,
               self.brand,
               self.car_type,
               self.stype,
               self.value,
               self.rolling_counter,
               self.time_stamp,
               self.version]

        vin, oem, brand, car_type, version = car_info[self.vin], \
                                           car_info[self.oem], \
                                           car_info[self.brand], \
                                           car_info[self.car_type], \
                                           car_info[self.version]

        gps_key = [self.vin,
                   self.oem,
                   self.brand,
                   self.car_type,
                   self.stype,
                   self.time_stamp,
                   self.lat,
                   self.lon,
                   self.bearing,
                   self.speed]

        ts = round(time.time() * 1000)
        gps_data = config.GPS_DATA
        gps_value = [vin,
                     oem,
                     brand,
                     car_type,
                     config.GPS_KEY_WORD,
                     str(ts),
                     gps_data[self.lat],
                     gps_data[self.lon],
                     gps_data[self.bearing],
                     gps_data[self.speed]]

        # 根据变量范围生成随机ECU数据
        ecu_data = self.generate_ecu_data(data_info)

        # 生成一次事件的全部数据（字典列表）
        counter = 0
        event_data_list = []  # 一次事件的全部数据

        try:
            for col, var in ecu_data.iteritems():
                stype_list = list(var.index)
                for st in range(len(stype_list)):
                    temp_value = [vin, oem, brand, car_type, stype_list[st], str(var[stype_list[st]]), str(col), str(ts), version]
                    temp_dict = dict(zip(key, temp_value))
                    json_message = json.dumps(temp_dict).replace(" ", "") + '\n'
                    save_file.write(json_message)
                    counter += 1
                    # InnerKafkaMessage.producer.send(config.TOPIC, bytes(str(json_message), 'utf-8'))
                    event_data_list.append(temp_dict)
                ts = ts + 100

            gps_dict = dict(zip(gps_key, gps_value))
            gps_json_message = json.dumps(gps_dict).replace(" ", "") + '\n'
            InnerKafkaMessage.producer.send(config.TOPIC, bytes(str(gps_json_message), 'utf-8'))
            time.sleep(1)
        except KafkaError as e:
            print(e)
        finally:
            save_file.close()
            print(counter)
            return event_data_list


if __name__ == "__main__":
    """文件输入输出"""
    file_path = config.FILE_PATH
    variable_input_file = config.VARIABLE_FILE
    test_output_file = config.OUTPUT_FILE

    """测试车辆信息"""
    car_information = config.CAR_INFO
    car_num = 1  # len(car_information)

    """功能信息"""
    function_list = config.FUNCTION_ZH
    function_attribute = config.VARIABLE_ATTRIBUTE
    variable_prefix = config.VARIABLE_PREFIX
    max_rolling = config.MAX_RECORD

    function_num = len(function_list)
    raw_data = [0] * function_num
    test_data_file = [0] * function_num
    io = pd.io.excel.ExcelFile(file_path + variable_input_file)

    for n in range(car_num):
        for i in range(function_num):
            raw_data[i] = pd.read_excel(io, sheet_name=function_list[i], skiprows=1)
            file_name = file_path + function_list[i] + test_output_file
            test_data_file[i] = open(file_name, 'w')
            kafka_message_instance = InnerKafkaMessage(max_rolling[i], raw_data[i].shape[0], variable_prefix[i])
            kafka_message_instance.generate_message_value(car_information[n], raw_data[i], test_data_file[i])


