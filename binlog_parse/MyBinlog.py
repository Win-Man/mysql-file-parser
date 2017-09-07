#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/4 15:02
# @Author  : gangshen
# @Site    :
# @File    : mybin.py
# @Software: PyCharm
import time
from .tools import *
from .MyEvent import *


class MyBinlog():

    def __init__(self,filename):
        self.filename = filename
        with open(filename,'rb') as f:
            self.bin_hex = f.read().encode('hex')
            self.filesize = len(self.bin_hex)/2

    #遍历binlog中的所有event，并解析输出
    def iter_event(self):
        event_postion = 4
        while event_postion + 1 <= self.filesize:
            next_event_position = self.get_next_event_position(event_postion)
            #print "next_position:%d" %(next_event_position)
            self.parse_event(event_postion, next_event_position)
            event_postion = next_event_position
            #print "file_size:%d" %(self.filesize)


    #解析binlog中的一个event
    def parse_event(self,event_position_byte,next_event_position_byte):
        event = MyEvent(self.bin_hex[event_position_byte*2:next_event_position_byte*2])

    #获取下一个event的起始位置点，返回结果为byte的偏移量
    def get_next_event_position(self,event_position_byte):
        next_position_hex = self.bin_hex[event_position_byte*2+13*2:event_position_byte*2+17*2]
        next_position = hex_to_str(next_position_hex)
        return hex_to_int(next_position)

if __name__ == '__main__':
    my = MyBinlog('mysql-bin.000002')
    my.iter_event()
    #print int('00000078',16)
    # single_binlog_parse('mysql-bin.000002')
    # print hex_to_str('12345678')
    # with open('mysql-bin.000002','rb') as f:
    #     bin_hex = f.read().encode('hex')
    #     print bin_hex[0:8]
    #     print "timestamp:%s" % bin_hex[8:16] # +8
    #     print "type_code:%s" % bin_hex[16:18] # +2
    #     print "server_id:%s" % bin_hex[18:26] # +8
    #     print "event_length:%s" % bin_hex[26:34] # +8
    #     print "next_position:%s" % bin_hex[34:42] # +8
    #     print "flags:%s" % bin_hex[42:46]
    #     print "extra_headers:"
    #     print '80188'.encode('hex')
    #     print "timestamp:%s" % bin_hex[240:248]
    #     print "type_code:%s" % bin_hex[248:250]
    #     print "server_id:%s" % bin_hex[250:258]
    #     print "event_length:%s" % bin_hex[258:266]