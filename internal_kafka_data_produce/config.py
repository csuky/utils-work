"""
-------------------------------------------------
# @Time     : 5/16/2020 4:21 PM
# @Author   : Kang Yang
# @Email    : yang.kang@uaes.com
# @Description：
-------------------------------------------------
"""

"""内部交互Kafka连接信息"""
HOST = ["172.29.30.236:9092", "172.29.30.237:9092", "172.29.30.238:9092"]
TOPIC = "GWM-CHB071-uaes-topic"

"""文件交互"""
FILE_PATH = "./data_file/"
VARIABLE_FILE = "云端信号.xlsx"
OUTPUT_FILE = "_测试数据.txt"

"""消息格式"""
VIN = "vin"
STYPE = "stype"
TIME_STAMP = "timestamp"
VALUE = "value"
ROLLING_COUNTER = "rollingCounter"
OEM = "oem"
BRAND = "brand"
CAR_TYPE = "carType"
VERSION = "version"
EVENT_PREFIX = ""  # ""event."

"""GPS"""
LAT = "lat"
LON = "long"
BEARING = "bearing"
SPEED = "speed"
GPS_KEY_WORD = "GPS"

"""功能信息"""
FUNCTION_ZH = ['怠速不稳', '加速不良', '起动困难']
FUNCTION_EN = ['idleUnstable', 'weakAccelerate', 'startDifficult']
VARIABLE_PREFIX = [EVENT_PREFIX + i + '.' for i in FUNCTION_EN]
MAX_RECORD = [30, 40, 40]
VARIABLE_ATTRIBUTE = ['云端变量名', '数据类型', '物理值上限', '物理值下限']

"""车辆信息"""
CAR_INFO = [
    {
        VIN: 'GWMTEST0000000001',
        OEM: 'GWM',
        BRAND: 'WEY',
        CAR_TYPE: 'CHB071',
        VERSION: '1',
        LAT: "29.60000",  # 拉萨
        LON: "91.00000",
        BEARING: "",
        SPEED: ""
    },
    {
        VIN: 'GWMTEST0000000002',
        OEM: 'GWM',
        BRAND: 'WEY',
        CAR_TYPE: 'P3011',
        VERSION: '1',
        LAT: "30.51667",  # 武汉
        LON: "114.31667",
        BEARING: "",
        SPEED: ""
    },
    {
        VIN: 'GWMTEST0000000003',
        OEM: 'GWM',
        BRAND: '哈弗',
        CAR_TYPE: 'H5',
        VERSION: '1',
        LAT: "36.56667",  # 西宁
        LON: "101.75000",
        BEARING: "",
        SPEED: ""
    },
    {
        VIN: 'GWMTEST0000000004',
        OEM: 'GWM',
        BRAND: '哈弗',
        CAR_TYPE: 'H6',
        VERSION: '1',
        LAT: "45.75000",  # 哈尔滨
        LON: "126.63333",
        BEARING: "",
        SPEED: ""
    }
]

