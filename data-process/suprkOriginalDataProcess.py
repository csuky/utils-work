# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 10:36:27 2019

@author: yang.kang
@desc: MongoDB中ECU变量相关字段长度提取
"""
import json
import re

originalFile = r'D:\KY\Code\Utils\utils-work\data-process\suprknk_MongoDB_12.28.txt'
processJsonFile = r'D:\KY\Code\Utils\utils-work\data-process\MongoDB_superKnock_jsonData.txt'

with open(originalFile, 'r') as inputFile:
    fileData = inputFile.read()

numRe = r'/\*\s\d+\s\*/\n'
numRet = re.sub(numRe, "", fileData)
huanhangRet = re.sub(r'\n|\s|\\', "", numRet)
huanhangRet = re.sub(r'ObjectId\("\w+"\)', '"id"', huanhangRet)
jsonSplitRe = r'\}\{'
jsonSplitRet = re.sub(jsonSplitRe, r'}\n{', huanhangRet)
jsonSplitRet = re.sub(r'"\{', '{', jsonSplitRet)
jsonSplitRet = re.sub(r'\}"', '}', jsonSplitRet)
jsonData = jsonSplitRet.split('\n')
payloadLen = []

for i in range(len(jsonData)):
    tempJson = json.loads(jsonData[i])
    # payloadKey = tempJson.key
    payloadLen.append(len(tempJson['payload']))
    # print(tempJson['payload'])
print(payloadLen)


# with open(processJsonFile, 'w') as outputFile:
#     outputFile.write(jsonRet)

