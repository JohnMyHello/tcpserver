# -*- coding=utf-8 -*-
import pymysql
# from camera_counter_msg import parse_msg
import logging
from DBUtils.PooledDB import PooledDB
from pymysql.cursors import DictCursor
import re
import time


class Mysql(object):
    __pool = None

    def __init__(self):
        config = {
            'creator': pymysql,
            'mincached': 1,
            'maxcached': 20,
            'host': "rm-wz9yv0u822d9v3h07o.mysql.rds.aliyuncs.com",
            'port': 3306,
            'user': "camera",
            'passwd': "!1q@2w#3e",
            'db': "hope",
            'use_unicode': False,
            'cursorclass': DictCursor,
        }
        self._conn = Mysql.__get_conn(config)
        self._cursor = self._conn.cursor()

    @staticmethod
    def __get_conn(config):
        if Mysql.__pool is None:
            __pool = PooledDB(**config)
            return __pool.connection()
        else:
            return False

    def insert(self, sql):
        self._cursor.execute(sql)
        return self.__get_insert_id()

    def query(self, sql):
        count = self._cursor.execute(sql)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    def __get_insert_id(self):
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

    def end(self, option='commit'):
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, is_end=1):
        if is_end == 1:
            self.end('commit')
        else:
            self.end('rollback')
        # self._cursor.close()
        # self._conn.close()

logging.basicConfig(level=logging.DEBUG,
                    format='%(thread)d %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='camera_counter_server.log',
                    filemode='a')

mysql = Mysql()


class ProcessData(object):
    def __init__(self):
        self.mac = ''
        self.flag = True
        self.flag_registered = False
        self.is_online_flag_count = 0

    @staticmethod
    def info_msg(info_msg):
        logging.info(info_msg)

    @staticmethod
    def debug_msg(err):
        waring_msg = "Insert error: %s" % err
        logging.debug(waring_msg)

    # 检查设备是否注册
    def is_registered(self):
        try:
            sql = 'SELECT id FROM camera_counter WHERE mac="%s"' % self.mac
            result = mysql.query(sql)
            if result:
                self.flag_registered = True
            else:
                msg = 'Device not registered! mac: %s' % self.mac
                logging.info(msg)
                self.flag = False
        except Exception as err:
            logging.debug(err)

    # 设置是否在线：
    #   1.  is_online=0 为不在线，默认
    #   2.  is_online=1 为在线
    def is_online(self, is_online=0):
        try:
            sql = "UPDATE camera_counter SET is_online=%d WHERE mac='%s'" % (is_online, self.mac)
            mysql.insert(sql)
            mysql.dispose(1)
        except Exception as err:
            logging.debug(err)

    # 检查数据的合法性
    def check(self, data):
        if len(data) > 10 and self.flag is True:
            if self.flag_registered is False:
                self.is_registered()
                if self.flag_registered is False:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return False

    # 解析数据
    def parse_msg(self, data):
        timestamp = ''
        data_type = ''
        try:
            if re.search(r'unitname=(.*?)\n', data):
                mac = re.search(r'unitname=(.*?)\n', data).group(1)
                if isinstance(mac, str):
                    self.mac = mac.replace(':', '')
            flag = self.check(data)
            if flag:
                if re.search(r'dts=(.*?)\n', data):
                    timestamp = re.search(r'dts=(.*?)\n', data).group(1)
                    timestamp = float(timestamp)
                if re.search(r'type=(.*?)&', data):
                    data_type = re.search(r'type=(.*?)&', data).group(1)
            return data_type, timestamp
        except Exception as err:
            debug_msg = 'Parse fail! err: %s' % err
            logging.debug(debug_msg)
            return data_type, timestamp

    # 根据数据的类型，分别处理
    def distributing_data(self, data):
        data_type, timestamp = self.parse_msg(data)

        # 处理心跳包
        if data_type == 'heartbeat':
            self.heartbeat()

        # 处理数据包
        elif data_type == 'counting':
            self.insert_msg(timestamp)

    # 处理心跳包
    def heartbeat(self):
        # 断线后立即重连会导致首次60s上传的心跳处理之后，100s的 timeout才会触发，使第一次的心跳更新会被timeout覆盖
        # 会导致在线状态异常，所以设置前三次心跳都更新设备的在线状态。
        # if self.is_online_flag_count <= 2:
            self.is_online(is_online=1)
            # self.is_online_flag_count += 1

    # 处理数据包
    def insert_msg(self, timestamp):
        try:
            if isinstance(timestamp, float):
                counter_time = time.localtime(timestamp)
                year = counter_time.tm_year
                month = counter_time.tm_mon
                day = counter_time.tm_mday
                hour = counter_time.tm_hour
                minute = counter_time.tm_min
                second = counter_time.tm_sec
                time_stamp = "%d-%d-%d %d:%d:%d" % (year, month, day, hour, minute, second)

                sql = 'INSERT INTO camera_counter_counting(mac, timestamp, year, month, day, hour, minute,' \
                      ' second, counting_in) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %s)' % \
                      (self.mac, time_stamp, year, month, day, hour, minute, second, 1)
                mysql.insert(sql)

                mysql.dispose(1)
                info_msg = "Insert success: %s" % self.mac
                logging.info(info_msg)

        except Exception, err:
            err = "Insert error: %s; mac:%s; timestamp:%s;" % (err, self.mac, timestamp)
            logging.debug(err)
