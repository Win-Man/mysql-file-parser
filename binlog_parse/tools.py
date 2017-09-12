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
        print "###################"
        print 'hex_str:%s' %(hex_str)
        print "###################"
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

if __name__ == '__main__':
    packed_integer('0100000000000000')