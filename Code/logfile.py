#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/10/21 20:47
# @Author : Drawon
# @Version :Python 3.7


import logging
import os
import socket
import traceback
import uuid
import time
from logging import handlers
from kafka import  KeyedProducer,KafkaProducer
from pandas.io import json

"""
log日志的写入

"""
hostName = socket.getfqdn(socket.gethostname())
host_ip = socket.gethostbyname(hostName)
event_id =uuid.uuid1()
# 可以将下面这些参数 配置到配置文件
kafka_use = True
BASE_DIR = os.path.abspath(os.getcwd())
LOG_DIR = os.path.join(BASE_DIR, "Logs/")
kafka_hosts = 'dn03.sit.abc.com.oa:9092,dn02.sit.abc.com.oa:9092,dn01.sit.abc.com.oa:9092'
hosts = kafka_hosts.split(',')
topics = 'Hotwheels-Realtime-Log'

JSON_LOGGING_FORMAT = json.dumps({
                                "eventId":"%(event_id)s",
                                "eventChannel": "%(app)s",
                                "hostName": "%(hostName)s",
                                "address": "%(ip)s",
                                "eventTime": "%(asctime)s",
                                "level": "%(levelname)s",
                                "message": "%(message)s",
                                "throwableInfo": "%(exc_text)s"
                                })

class JsonLoggingFilter(logging.Filter):
    def __init__(self, name):
        logging.Filter.__init__(self, name=name)
        self.ip =host_ip
        self.hostName = hostName
        self.app = name
        self.event_id = event_id


    def filter(self, record):
        record.ip = self.ip
        record.app = self.app
        record.hostName = self.hostName
        record.event_id = self.event_id
        # 为record 添加异常堆栈信息字段; 当有多个handler 的时候，这里会判断多次
        if hasattr(record, "stack_msg") and hasattr(record, "stack_trace"):
            return True

        if record.exc_info:
            ex_type, ex_val, ex_stack = record.exc_info
            stack_list = []
            for stack in traceback.extract_tb(ex_stack):
                stack_list.append("%s" % stack)

            record.stack_msg = ex_val
            record.stack_trace = "#".join(stack_list)
        else:
            record.stack_msg, record.stack_trace = "", ""

        return True


class JsonFormatter(logging.Formatter):
    """
    json格式化
    """
    def __init__(self, fmt=None):
        logging.Formatter.__init__(self, fmt=fmt)
    def format(self, record):
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        if record.exc_info and not record.exc_text:
        # Cache the traceback text to avoid converting it multiple times
        # (it‘s constant anyway)
            record.exc_text = self.formatException(record.exc_info).replace("\n", " ").replace("\"", "‘")

        s = self.formatMessage(record)
        return s

class KafkaLoggingHandler(logging.Handler):
    """
    形成 kafka 日志handle
    """
    def __init__(self,host,topic, **kwargs):
        logging.Handler.__init__(self)
        self.key = kwargs.get("key", None)
        self.kafka_topic_name = topic

        if not self.key:
            self.producer = KafkaProducer(bootstrap_servers=host,api_version=(0, 10, 1),**kwargs)
        else:
            self.producer = KeyedProducer(bootstrap_servers=host,api_version=(0, 10, 1),**kwargs)

    def emit(self, record):
        # 忽略kafka的日志，以免导致无限递归。
        if 'kafka' in record.name:
            return
        try:
            # 格式化日志并指定编码为utf-8
            print(f'record : {record}')
            message = {'eventId':str(event_id),"eventChannel":record.name,'hostName':hostName,'address':host_ip,'eventTime':time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(record.created)),'level':record.levelname,
                       'message':record.msg,'throwableInfo':record.exc_text}
            mess = json.dumps(message)
            mess = bytes(mess,encoding='utf8')
            # msg = self.format(record)
            # if isinstance(msg, unicode):
            #     msg = msg.encode("utf-8")
            # # kafka生产者，发送消息到broker。
            if not self.key:
                self.producer.send(self.kafka_topic_name,None,mess)
            else:
                self.producer.send(self.kafka_topic_name, self.key, mess)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)



class JsonLogger(logging.Logger):
    logger = None
    level = None
    mode = None
    _first_init = False

    # 满足只产生一个单例
    def __new__(cls, *args, **kwargs):
        if  cls.logger is None:
            cls.logger = object.__new__(cls)
        return cls.logger
    def __init__(self, app_name, level=logging.INFO, console_level=logging.INFO):
        # 创建单例时，只初始化一次__init__方法
        if not self._first_init:
            self.name = app_name
            self.app_name = app_name
            logging.Logger.__init__(self, name=app_name)
            JsonLogger._first_init = True

        self.logger = logging.Logger(name=app_name)
        self.logger.setLevel(level)
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        timenow = time.strftime('%Y%m%d', time.localtime(time.time()))
        log_file_path = LOG_DIR + timenow +'.log'
        json_logging_filter = JsonLoggingFilter(self.app_name)
        json_formatter = JsonFormatter(JSON_LOGGING_FORMAT)

        # 文件日志
        file_handle = handlers.TimedRotatingFileHandler(log_file_path, when='D',backupCount=30,encoding='utf-8')
        file_handle.setLevel(level)
        file_handle.setFormatter(json_formatter)
        file_handle.addFilter(json_logging_filter)
        # 控制台日志
        console_handle = logging.StreamHandler()
        console_handle.setLevel(console_level)
        console_handle.setFormatter(json_formatter)
        console_handle.addFilter(json_logging_filter)
        # kafka日志
        kafka_handle = KafkaLoggingHandler(hosts,topics)
        kafka_handle.setLevel(level)
        kafka_handle.addFilter(json_logging_filter)

        self.logger.addHandler(file_handle)
        self.logger.addHandler(console_handle)
        if kafka_use:
            self.logger.addHandler(kafka_handle)

    def setLevel(self, level):
        self.logger.level = level

    def getLogger(self):
        return self.logger

def loginfo(app_name='ml'):
    return JsonLogger(app_name).getLogger()




