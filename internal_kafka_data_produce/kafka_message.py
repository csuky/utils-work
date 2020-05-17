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
import config


class InnerKafkaMessage(object):
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
        # 初始化记录数、变量个数、变量名前缀
        self.record_number = record_number
        self.cloud_variable_num = var_num
        self.stype_prefix = var_prefix

    """根据变量类型和物理上下限，随机生成数据，series为series"""

    @staticmethod
    def generate_random_value(series, value_type, up_limit, low_limit):
        if series[value_type].upper() == 'BIT':
            result = random.randint(0, 1)
        else:
            result = random.uniform(series[up_limit], series[low_limit])
        return result

    """根据每个异常事件的变量属性表，生成不同rc下的满足物理范围的随机数据"""
    def generate_ecu_data(self, var_attr_data):
        var_attr = config.VARIABLE_ATTRIBUTE
        ecu_data_columns = list(range(self.record_number))
        stype_value = self.stype_prefix + var_attr_data[var_attr[0]]
        ecu_data_indexes = stype_value.tolist()
        # 索引为云端变量名，列名为rc
        ecu_data = pd.DataFrame(index=ecu_data_indexes, columns=ecu_data_columns)
        # 设置不同rc值下的value
        for j in ecu_data_columns:
            ecu_data[ecu_data_columns[j]] = var_attr_data.apply(InnerKafkaMessage.generate_random_value,
                                                                args=(var_attr[1],
                                                                      var_attr[2],
                                                                      var_attr[3]),
                                                                axis=1).tolist()
        return ecu_data

    def generate_message_value(self, car_info, rc, variable_data, ts):
        key = [self.vin,
               self.oem,
               self.brand,
               self.car_type,
               self.stype,
               self.value,
               self.rolling_counter,
               self.time_stamp]
        vin, oem, brand, car_type = car_info[self.vin], \
                                    car_info[self.oem], \
                                    car_info[self.brand], \
                                    car_info[self.car_type]
        stype_list = list(variable_data.index)
        value_list = []
        for st in range(len(stype_list)):
            temp_value = [vin, oem, brand, car_type, stype_list[st], str(variable_data[stype_list[st]]), str(rc),
                          str(ts)]
            temp_dict = dict(zip(key, temp_value))
            # message_json = json.dumps(temp_dict)
            value_list.append(temp_dict)
        return value_list


if __name__ == "__main__":
    file_path = "./云端信号.xlsx"
    io = pd.io.excel.ExcelFile(file_path)
    function_num = len(config.FUNCTION_ZH)
    raw_data = [0] * function_num
    car_information = config.CAR_INFO
    car_num = 1  # len(vehicle_info)

    for i in range(function_num):
        raw_data[i] = pd.read_excel(io, sheet_name=config.FUNCTION_ZH[i], skiprows=1)

    idle_unstable = InnerKafkaMessage(30, raw_data[0].shape[0], config.VARIABLE_PREFIX[0])
    weak_accelerate = InnerKafkaMessage(40, raw_data[1].shape[0], config.VARIABLE_PREFIX[1])
    start_difficult = InnerKafkaMessage(3, raw_data[2].shape[0], config.VARIABLE_PREFIX[2])
    idle_unstable_data = idle_unstable.generate_ecu_data(raw_data[0])
    weak_accelerate_data = weak_accelerate.generate_ecu_data(raw_data[1])
    start_difficult_data = start_difficult.generate_ecu_data(raw_data[2])
    ecu_data_list = [idle_unstable_data, weak_accelerate_data, start_difficult_data]

    data_list = []
    for n in range(car_num):
        for fun in range(2, function_num):
            time_stamp = round(time.time() * 1000)
            for col, var in ecu_data_list[fun].iteritems():
                one_group_list = start_difficult.generate_message_value(car_information[n], col, var, time_stamp)
                data_list.extend(one_group_list)
                time_stamp = time_stamp + 100
