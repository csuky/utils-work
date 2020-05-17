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

    def __init__(self):
        self.vin = config.VIN
        self.stype = config.STYPE
        self.time_stamp = config.TIME_STAMP
        self.value = config.VALUE
        self.rolling_counter = config.ROLLING_COUNTER
        self.oem = config.OEM
        self.brand = config.BRAND
        self.car_type = config.CAR_TYPE

    @staticmethod
    def generate_value(value_type, up_limit, low_limit):
        if value_type == "":
            result = random.randint(0, 1)
        else:
            result = random.uniform(up_limit, low_limit)
        return result

    def set_variable(self, prefix_name, variable_attribute, rc):
        # rolling_counter = [str(i) for i in range(rc)]
        raw_data_columns = ['云端变量名', '数据类型', '物理值上限', '物理值下限']
        rolling_counter = list(range(rc))
        ecu_data_columns = ["variable_name"] + rolling_counter
        stype_value = prefix_name + variable_attribute[raw_data_columns[0]]
        ecu_data_indexs = stype_value.value + [self.time_stamp]
        ecu_data = pd.DataFrame(index=ecu_data_indexs, columns=ecu_data_columns)
        # # 设置stype
        # ecu_data[ecu_data_columns[0]] =

        # 设置不同rc值下的value
        for i in rolling_counter:
            ecu_data[ecu_data_columns[i+1]] = variable_attribute.apply(InnerKafkaMessage.generate_value(variable_attribute[1],
                                                                                                        variable_attribute[2],
                                                                                                        variable_attribute[3]))
        return ecu_data

    def generate_message(self, value_list):
        key = [self.vin,
               self.stype,
               self.time_stamp,
               self.value,
               self.rolling_counter,
               self.oem,
               self.brand,
               self.car_type]
        value = [str(i) for i in value_list]
        message_dict = dict(zip(key, value))
        result = json.dumps(message_dict)
        return result


if __name__ == "__main__":
    file_path = "./云端信号.xlsx"
    io = pd.io.excel.ExcelFile(file_path)
    raw_data = [0, 0, 0]

    for i in range(len(config.FUNCTION_ZH)):
        raw_data[i] = pd.read_excel(io, sheet_name=config.FUNCTION_ZH[i], skiprows=1)

    idle_unstable_message = InnerKafkaMessage()
    weak_accelerate_message = InnerKafkaMessage()
    start_difficult_message = InnerKafkaMessage()

    start_difficult_message.set_variable('event.startDifficult', raw_data[2], 10)




