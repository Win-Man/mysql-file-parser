#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/6 10:37
# @Author  : gangshen
# @Site    : 
# @File    : tools.py
# @Software: PyCharm
import time

#将十六进制的字符串调整顺序
def hex_to_str(hex_str):
    hex_list = []
    for i in range(len(hex_str)/2):
        hex_list.append(hex_str[i*2:i*2+2])
    return ''.join(hex_list[::-1])

#将十六进制字符串转换为int
def hex_to_int(hex_str):
    try:
        res = int(hex_str,16)
    except Exception as e:
        print 'convert hex to int failed!!!\nThe string is %s.' %(hex_str)
    return res

def timestamp_to_str(timestamp,format):
    t = time.localtime(timestamp)
    return time.strftime(format,t)

def hex_to_ascii(hex_str):
    res = []
    for i in range(len(hex_str)/2):
        res.append(chr(int(hex_str[i*2:i*2+2],16)))
    return ''.join(res)

def packed_integer(hex_str):
    print hex_to_int(hex_str)

def get_packaged_int(hex_str):
    first_byte = hex_to_int(hex_str[:2])
    if first_byte <= 250:
        return (1,first_byte)
    elif first_byte == 252:
        return (2,hex_to_int(hex_to_str(hex_str[:4])))
    elif first_byte == 253:
        return (3,hex_to_int(hex_to_str(hex_str[:6])))
    elif first_byte == 254:
        return (8,hex_to_int(hex_to_str(hex_str[:8])))
    else:
        return None

def get_column_type_and_metadata(flag):
    if flag == 0:
        return ['MYSQL_TYPE_DECIMAL',0]
    elif flag == 1:
        return ['MYSQL_TYPE_TINY',0]
    elif flag == 2:
        return ['MYSQL_TYPE_SHORT', 0]
    elif flag == 3:
        return ['MYSQL_TYPE_LONG', 0]
    elif flag == 4:
        return ['MYSQL_TYPE_FLOAT', 1]
    elif flag == 5:
        return ['MYSQL_TYPE_DOUBLE', 1]
    elif flag == 6:
        return ['MYSQL_TYPE_NULL', 0]
    elif flag == 7:
        return ['MYSQL_TYPE_TIMESTAMP', 0]
    elif flag == 8:
        return ['MYSQL_TYPE_LONGLONG', 0]
    elif flag == 9:
        return ['MYSQL_TYPE_INT24', 0]
    elif flag == 10:
        return ['MYSQL_TYPE_DATE', 0]
    elif flag == 11:
        return ['MYSQL_TYPE_TIME', 0]
    elif flag == 12:
        return ['MYSQL_TYPE_DATETIME', 0]
    elif flag == 13:
        return ['MYSQL_TYPE_YEAR', 0]
    elif flag == 14:
        return ['MYSQL_TYPE_NEWDATE', 0]
    elif flag == 15:
        return ['MYSQL_TYPE_VARCHAR', 2]
    elif flag == 16:
        return ['MYSQL_TYPE_BIT', 2]
    elif flag == 246:
        return ['MYSQL_TYPE_NEWDECIMAL', 2]
    elif flag == 247:
        return ['MYSQL_TYPE_ENUM', 0]
    elif flag == 248:
        return ['MYSQL_TYPE_SET', 0]
    elif flag == 249:
        return ['MYSQL_TYPE_TINY_BLOB', 0]
    elif flag == 250:
        return ['MYSQL_TYPE_MEDIUM_BLOB', 0]
    elif flag == 251:
        return ['MYSQL_TYPE_LONG_BLOB', 0]
    elif flag == 252:
        return ['MYSQL_TYPE_BLOB', 1]
    elif flag == 253:
        return ['MYSQL_TYPE_VAR_STRING', 2]
    elif flag == 254:
        return ['MYSQL_TYPE_STRING', 2]
    elif flag == 255:
        return ['MYSQL_TYPE_GEOMETRY', 1]

if __name__ == '__main__':
    packed_integer('0100000000000000')