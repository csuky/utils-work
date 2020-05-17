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
EVENT_PREFIX = "event."
FUNCTION_ZH = ['怠速不稳', '加速不良', '起动困难']
FUNCTION_EN = ['idleUnstable', 'weakAccelerate', 'startDifficult']
VARIABLE_PREFIX = [EVENT_PREFIX+i+'.' for i in FUNCTION_EN]
MAX_RC = {
    FUNCTION_EN[0]: 30,
    FUNCTION_EN[1]: 40,
    FUNCTION_EN[2]: 40
}

"""Kafka链接信息"""
HOST = ""
TOPIC = ""

"""车辆信息"""
CAR_INFO = [
    {
        VIN: 'TEST0000001',
        OEM: 'GWM',
        BRAND: 'WEY',
        CAR_TYPE: 'CHB071',
    },
    {
        VIN: 'TEST0000002',
        OEM: 'GWM',
        BRAND: 'WEY',
        CAR_TYPE: 'P3011',
    },
    {
        VIN: 'TEST0000003',
        OEM: 'GWM',
        BRAND: '哈弗',
        CAR_TYPE: 'H5',
    },
    {
        VIN: 'TEST0000004',
        OEM: 'GWM',
        BRAND: '哈弗',
        CAR_TYPE: 'H6',
    },
]

