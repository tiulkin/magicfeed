from amqp.basic_message import Message
from amqp import Connection
from tornado.escape import json_encode
from loggers import loggers
from constants import *
import time
channel = None

queue_types = (
    "channels.lj.add",
    "channels.lj.process",
    "channels.lj.update",
    "channels.lj.items",
    "channels.lj.custom_1",
    "channels.rss.add",
    "channels.rss.process",
    "channels.rss.update",
    "channels.rss.items",
    "channels.rss.custom_1",
    "channels.vk.add",
    "channels.vk.process",
    "channels.vk.update",
    "channels.vk.items",
    "channels.vk.custom_1",
    "channels.vk.custom_2",
    "channels.clean.custom_1",
    "channels.replication.add")




def send_message(queue_name,message):
    global channel
    if not channel:
        init_rmq()

    body = json_encode(message)
    done = False
    while not done:
        try:
            message = Message(body, content_type='text/plain')
            channel.basic_publish(message, exchange="main_exchange", routing_key=queue_name)
            done = True
            loggers["messages"]["info"]["logger"].debug("queue_name: %s", queue_name)
            loggers["messages"]["info"]["logger"].debug("message: %s", body)

        except Exception as err:
            loggers["messages"]["errors"]["logger"].error(err)
            loggers["messages"]["errors"]["logger"].error("routing_key: %s", queue_name)
            loggers["messages"]["errors"]["logger"].error('Trying to reinit RMQ...')
            time.sleep(3)
            init_rmq(True)
            loggers["messages"]["errors"]["logger"].error('...reinited')


def init_rmq(force=False, queue_types = queue_types):
    global channel
    connected = False
    if not channel or force:
        connection = Connection(host=RMQ_SERVER["host"], userid=RMQ_SERVER["credentials"][0],
                                password=RMQ_SERVER["credentials"][1], heartbeat=30, virtual_host="/", connect_timeout=5)
        tmp_channel = connection.channel()
        tmp_channel.basic_qos(0, 1, True)
        tmp_channel.exchange_declare(exchange="main_exchange", type="direct",
                                     passive=False, durable=True, auto_delete=False)

    while not connected:
        try:
            for channel_type in queue_types:
                queueName = channel_type
                tmp_channel.queue_declare(queueName, durable=True, auto_delete=False)
                tmp_channel.queue_bind(queue=queueName,
                                       exchange="main_exchange",
                                       routing_key=queueName)

            connected = True
        except:
            print("waiting...")
            time.sleep(3)

    else:
        channel = tmp_channel
