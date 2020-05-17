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
        self.max_rc = 0
        self.stype_prefix = ""

    @staticmethod
    def generate_random_value(data_frame, value_type, up_limit, low_limit):
        # value_type, up_limit, low_limit = data_frame[]
        if data_frame[value_type].upper() == 'BIT':
            result = random.randint(0, 1)
        else:
            result = random.uniform(data_frame[up_limit], data_frame[low_limit])
        return result

    @staticmethod
    def generate_ecu_data(prefix_name, variable_attribute, rc):
        raw_data_columns = ['云端变量名', '数据类型', '物理值上限', '物理值下限']
        rolling_counter = list(range(rc))
        ecu_data_columns = rolling_counter
        stype_value = prefix_name + variable_attribute[raw_data_columns[0]]
        ecu_data_indexes = stype_value.tolist()
        ecu_data = pd.DataFrame(index=ecu_data_indexes, columns=ecu_data_columns)

        # 设置不同rc值下的value
        for j in rolling_counter:
            ecu_data[ecu_data_columns[j]] = variable_attribute.apply(InnerKafkaMessage.generate_random_value,
                                                                     args=(raw_data_columns[1],
                                                                           raw_data_columns[2],
                                                                           raw_data_columns[3]),
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
            temp_value = [vin, oem, brand, car_type, stype_list[st], str(variable_data[stype_list[st]]), str(rc), str(ts)]
            temp_dict = dict(zip(key, temp_value))
            # message_json = json.dumps(temp_dict)
            value_list.append(temp_dict)
        return value_list

    # def generate_message(self, value_list):
    #     key = [self.vin,
    #            self.stype,
    #            self.time_stamp,
    #            self.value,
    #            self.rolling_counter,
    #            self.oem,
    #            self.brand,
    #            self.car_type]
    #     value = [str(k) for k in value_list]
    #     message_dict = dict(zip(key, value))
    #     result = json.dumps(message_dict)
    #     return result


if __name__ == "__main__":
    file_path = "./云端信号.xlsx"
    io = pd.io.excel.ExcelFile(file_path)
    function_num = len(config.FUNCTION_ZH)
    raw_data = [0]*function_num
    car_info = config.CAR_INFO
    car_num = 1  # len(vehicle_info)

    for i in range(function_num):
        raw_data[i] = pd.read_excel(io, sheet_name=config.FUNCTION_ZH[i], skiprows=1)
    idle_unstable = InnerKafkaMessage()
    weak_accelerate = InnerKafkaMessage()
    start_difficult = InnerKafkaMessage()
    idle_unstable_data = InnerKafkaMessage().generate_ecu_data(config.VARIABLE_PREFIX[0], raw_data[0], 10)
    weak_accelerate_data = InnerKafkaMessage().generate_ecu_data(config.VARIABLE_PREFIX[1], raw_data[1], 10)
    start_difficult_data = InnerKafkaMessage().generate_ecu_data(config.VARIABLE_PREFIX[2], raw_data[2], 2)

    # start_difficult_data = start_difficult_message.generate_ecu_data(config.VARIABLE_PREFIX[2], raw_data[2], 10)
    ecu_data_list = [idle_unstable_data, weak_accelerate_data, start_difficult_data]
    data_list = []
    for n in range(car_num):
        for fun in range(2, function_num):
            time_stamp = round(time.time() * 1000)
            for rc, var in ecu_data_list[fun].iteritems():
                one_group_list = start_difficult.generate_message_value(car_info[n], rc, var, time_stamp)
                data_list.extend(one_group_list)
                time_stamp = time_stamp + 100





