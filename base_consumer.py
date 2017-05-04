"""
Базовый класс микросервиса
"""
from tornado import gen, httpclient
from tornado.ioloop import IOLoop
from tornado.escape import json_encode
from loggers import loggers
from constants import *
import time
import traceback
from azure.servicebus import ServiceBusService
from azure.servicebus import Message
from azure.common import (
    AzureHttpError
)

class BaseConsumer(object):
    def __init__(self, queue_type):
        self.channel = None
        self.connection = None
        self.queue_type = queue_type
        self.bus_service = ServiceBusService(SERVICE_BUS_NAME,
                                        shared_access_key_name=SHARED_ACCESS_KEY_NAME,
                                        shared_access_key_value=SHARED_ACCESS_KEY_VALUE, timeout=165)

    def queue_callback(self,message):
        pass

    def empty_callback(self):
        pass

    def send_message(self, queue_name, message):
        done = False
        while not done:
            try:
                body = json_encode(message)
                service_message = Message(body.encode('utf_8'))
                self.bus_service.send_queue_message(queue_name, service_message)
                done = True
                loggers["messages"]["info"]["logger"].debug("queue_name: %s", queue_name)
                loggers["messages"]["info"]["logger"].debug("message: %s", body)

            except Exception as err:
                loggers["messages"]["errors"]["logger"].error(err)
                loggers["messages"]["errors"]["logger"].error("routing_key: %s", queue_name)
                loggers["messages"]["errors"]["logger"].error('Trying to reinit RMQ...')
                time.sleep(3)
                loggers["messages"]["errors"]["logger"].error('...reinited')

    def start_consuming(self):
        message = None
        while True:
            try:
                message = self.bus_service.receive_queue_message(self.queue_type , peek_lock=True, timeout=60)
                if message and message.body:
                    self.queue_callback(message)
                else:
                    self.empty_callback()
                    time.sleep(5)

            except Exception as err:
                loggers["messages"]["errors"]["logger"].error("consuming")
                loggers["messages"]["errors"]["logger"].error(err)
                time.sleep(5)





