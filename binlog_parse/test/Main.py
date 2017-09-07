#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/6 10:41
# @Author  : gangshen
# @Site    : 
# @File    : Main.py
# @Software: PyCharm
from binlog_parse.MyBinlog import MyBinlog

if __name__ == '__main__':
    my = MyBinlog('mysql-bin.000004')
    my.iter_event()