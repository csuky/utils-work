"""
-------------------------------------------------
# @Time     : 5/16/2020 4:21 PM
# @Author   : Kang Yang
# @Email    : yang.kang@uaes.com
# @Description：
-------------------------------------------------
"""

"""消息格式"""
VIN = "vin"
STYPE = "stype"
TIME_STAMP = "timestamp"
VALUE = "value"
ROLLING_COUNTER = "rollingCounter"
OEM = "oem"
BRAND = "brand"
CAR_TYPE = "carType"
FUNCTION_PREFIX = "event."
MAX_RC = {
    "idleUnstable": 30,
    "weakAccelerate": 40,
    "startDifficult": 40
}
FUNCTION_ZH = ['怠速不稳', '加速不良', '起动困难']

"""Kafka链接信息"""
HOST = ""
TOPIC = ""

