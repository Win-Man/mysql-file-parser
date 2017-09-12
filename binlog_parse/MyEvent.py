#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/6 14:42
# @Author  : gangshen
# @Site    : 
# @File    : MyEvent.py
# @Software: PyCharm

from binlog_parse.tools import *


class MyEvent():
    def __init__(self,event_hex):
        self.event_hex = event_hex
        # event存放16进制数据
        self.event = {}
        self.__paser_event_v4__(self.event_hex)

    def __paser_event_v4__(self,event_hex):
        self.event['type_code'] = hex_to_str(event_hex[8:10])
        if hex_to_int(self.event['type_code']) == 0:
            self.event['name'] = 'UNKNOW EVENT'
            self.__unknow_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 1:
            self.event['name'] = 'START EVENT V3'
            self.__start_event_v3(event_hex)
        elif hex_to_int(self.event['type_code']) == 2:
            self.event['name'] = 'QUERY EVENT'
            self.__query_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 3:
            self.event['name'] = 'STOP EVENT'
            self.__stop_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 4:
            self.event['name'] = 'ROTATE EVENT'
            self.__rotate_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 5:
            self.event['name'] = 'INTVAR EVENT'
            self.__intvar_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 6:
            self.event['name'] = 'LOAD EVENT'
            self.__load_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 7:
            self.event['name'] = 'SLAVE EVENT'
            self.__slave_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 8:
            self.event['name'] = 'CREATE FILE EVENT'
            self.__create_file_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 9:
            self.event['name'] = 'OPEN BLOCK EVENT'
            self.__append_block_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 10:
            self.event['name'] = 'EXEC LOAD EVENT'
            self.__exec_load_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 11:
            self.event['name'] = 'DELETE FILE EVENT'
            self.__delete_file_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 12:
            self.event['name'] = 'NEW LOAD EVENT'
            self.__new_load_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 13:
            self.event['name'] = 'RAND EVENT'
            self.__rand_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 14:
            self.event['name'] = 'USER VAR EVENT'
            self.__user_var_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 15:
            self.event['name'] = 'FORMAT DESCRIPTION EVENT'
            self.__format_description_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 16 :
            self.event['name'] = 'XID EVENT'
            self.__xid_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 17:
            self.event['name'] = 'BEGIN LOAD QUERY EVENT'
            self.__begin_load_query_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 18:
            self.event['name'] = 'EXECUTE LOAD QUERY EVENT'
            self.__execute_load_query_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 19:
            self.event['name'] = 'TABLE MAP EVENT'
            self.__table_map_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 20:
            self.event['name'] = 'PRE GA WRITE ROWS EVENT'
            self.__pre_ga_write_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 21:
            self.event['name'] = 'PRE GA UPDATE ROWS EVENT'
            self.__pre_ga_update_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 22:
            self.event['name'] = 'PRE GA DELETE ROWS EVENT'
            self.__pre_ga_delete_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 23:
            self.event['name'] = 'WRITE ROWS EVENT V1'
            self.__write_rows_event_v1__(event_hex)
        elif hex_to_int(self.event['type_code']) == 24:
            self.event['name'] = 'UPDATE ROWS EVENT V1'
            self.__update_rows_event_v1__(event_hex)
        elif hex_to_int(self.event['type_code']) == 25:
            self.event['name'] = 'DELETE ROWS EVENT V1'
            self.__delete_rows_event_v1__(event_hex)
        elif hex_to_int(self.event['type_code']) == 26:
            self.event['name'] = 'INCIDENT EVENT'
            self.__incident_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 27:
            self.event['name'] = 'HEARTBEAT LOG EVENT'
            self.__heartbeat_log_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 28:
            self.event['name'] = 'IGNORABLE LOG EVENT'
            self.__ignorable_log_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 29:
            self.event['name'] = 'ROWS QUERY LOG EVENT'
            self.__rows_query_log_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 30:
            self.event['name'] = 'WRITE ROWS EVENT'
            self.__write_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 31:
            self.event['name'] = 'UPDATE ROWS EVENT'
            self.__update_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 32:
            self.event['name'] = 'DELETE ROWS EVENT'
            self.__delete_rows_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 33:
            self.event['name'] = 'GTID EVENT'
            self.__gtid_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 34:
            self.event['name'] = 'ANONYMOUS GTID LOG EVENT'
            self.__anonymous_gtid_log_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 35:
            self.event['name'] = 'PREVIOUS GTIDS LOG EVENT'
            self.__previous_gtids_log_event(event_hex)
        elif hex_to_int(self.event['type_code']) == 36:
            self.event['name'] = 'TRANSACTION CONTEXT EVENT'
            self.__transaction_context_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 37:
            self.event['name'] = 'VIEW CHANGE EVENT'
            self.__view_change_event__(event_hex)
        elif hex_to_int(self.event['type_code']) == 38:
            self.event['name'] = 'XA PREPARE LOG EVENT'
            self.__xa_prepare_log_event__(event_hex)
        else:
            self.event = None
        if self.event != None:
            print self.event

    # BINLOG_CHECKSUM_LEN = 4
    # BINLOG_CHECKSUM_ALG_DESC_LEN = 1
    # format_description_event的checksum是5个字节的，其他的event的checksum是4个字节的
    def __format_description_event__(self,event_hex):
        # event_header
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])),"%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        # event data
        self.event['binlog_version'] = hex_to_int(hex_to_str(event_hex[38:42])) # 2
        self.event['server_version'] = hex_to_ascii(event_hex[42:142])# 不用倒序，50
        #self.event['server_version'] = event_hex[42:142] # 不用倒序，
        self.event['create_timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[142:150])), "%Y-%m-%d %H:%M:%S") #4
        self.event['header_length'] = hex_to_int(hex_to_str(event_hex[150:152]))
        self.event['post_header_len'] = event_hex[152:228]
        self.event['check_sum'] = event_hex[228:]

    def __xid_event__(self,event_hex):
        # event_header
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None
        self.event['xid'] = hex_to_int(hex_to_str(event_hex[38:54]))
        self.event['check_sum'] = event_hex[54:]

    def __previous_gtids_log_event(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None
        self.event['unknow'] = event_hex[38:]

    def __gtid_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:122]
        self.event['unknow'] = event_hex[122:]

    def __rotate_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]
        self.event['next_binlog_file_name'] = hex_to_ascii(event_hex[54:86])
        self.event['pos'] = hex_to_int(hex_to_str(event_hex[86:102]))

    def __unknow_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:150]


    def __start_event_v3(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:64]

    def __query_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __stop_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]

    def __intvar_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __load_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:74]

    def __slave_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __create_file_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:46]

    def __append_block_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:46]

    def __exec_load_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:46]

    def __delete_file_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:46]

    def __new_load_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:74]

    def __rand_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __user_var_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __begin_load_query_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:46]

    def __execute_load_query_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:64]

    def __table_map_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]

    def __pre_ga_write_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __pre_ga_update_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __pre_ga_delete_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __write_rows_event_v1__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]

    def __update_rows_event_v1__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]

    def __delete_rows_event_v1__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:54]

    def __incident_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:42]

    def __heartbeat_log_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __ignorable_log_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __rows_query_log_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __write_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:58]

    def __update_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:58]

    def __delete_rows_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:58]

    def __anonymous_gtid_log_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:122]

    def __transaction_context_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:74]

    def __view_change_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = event_hex[38:142]

    def __xa_prepare_log_event__(self,event_hex):
        self.event['timestamp'] = timestamp_to_str(hex_to_int(hex_to_str(event_hex[0:8])), "%Y-%m-%d %H:%M:%S")
        self.event['type_code'] = hex_to_int(hex_to_str(event_hex[8:10]))
        self.event['server_id'] = hex_to_int(hex_to_str(event_hex[10:18]))
        self.event['event_length'] = hex_to_int(hex_to_str(event_hex[18:26]))
        self.event['next_position'] = hex_to_int(hex_to_str(event_hex[26:34]))
        self.event['flags'] = hex_to_int(hex_to_str(event_hex[34:38]))
        self.event['extra_headers'] = None

    def __print_event__(self):
        if self.event != None:
            for (k,v) in self.event.items():
                print "%s:%s" %(k,v)