# !/usr/bin/env python3

import time
import tornado
import couchdb
import traceback
import sys
from tornado.escape import json_encode
from uuid import uuid4
from loggers import loggers
from tornado import ioloop
from constants import *
from couchdb import http
from pymongo import *
from pymongo.errors import NetworkTimeout

REPLICATION_PERIOD = 60000
replication_statuses = {}
target_server_id = "azure"

target_server_url = "http://" + COUCH_USERS_CREDS["creds"] + "@" + COUCH_SERVERS[target_server_id]["host"]
internal_server_url = "http://" + COUCH_INTERNAL_CREDS["creds"] + "@" + COUCH_SERVERS["azure_internal"]["host"]
# source_server_url = "http://" + COUCH_CHANNELS_CREDS["creds"] + "@" + COUCH_SERVERS[source_server_id]["host"]
server = couchdb.client.Server(internal_server_url, session=http.Session(timeout=60))
server.init_time = time.time()
# channels_db = server[COUCH_CHANNELS_LIST_PRIVATE["database"]]
requests_db = server["requests"]
internal_server = couchdb.client.Server(internal_server_url, session=http.Session(timeout=60))
internal_server.init_time = time.time()
client = MongoClient(MONGO_SERVER_IP, socketKeepAlive = True , socketTimeoutMS = 30000)
db = client.mf
channels_queue = db.ChannelsQueue
articles_queue = db.ArticlesImportLog
vk_tasks = db.CustomWork
alerts_list = db.Alerts


connection = None
channel = None
# from init_queues import *

from azure.servicebus import ServiceBusService
from azure.servicebus import Message
bus_service = ServiceBusService(SERVICE_BUS_NAME,
                                shared_access_key_name=SHARED_ACCESS_KEY_NAME,
                                shared_access_key_value=SHARED_ACCESS_KEY_VALUE)


def send_message(queue_name,message) :
    global bus_service
    done = False
    while not done:
        try:
            body = json_encode(message)
            service_message = Message(body.encode('utf_8'))
            bus_service.send_queue_message(queue_name, service_message)
            done = True
            loggers["messages"]["info"]["logger"].debug("queue_name: %s", queue_name)
            loggers["messages"]["info"]["logger"].debug("message: %s", body)

        except Exception as err:
            loggers["messages"]["errors"]["logger"].error(err)
            loggers["messages"]["errors"]["logger"].error("routing_key: %s", queue_name)
            loggers["messages"]["errors"]["logger"].error('Trying to reinit RMQ...')
            time.sleep(3)
            # bus_service = ServiceBusService(SERVICE_BUS_NAME,
            #                                 shared_access_key_name=SHARED_ACCESS_KEY_NAME,
            #                                 shared_access_key_value=SHARED_ACCESS_KEY_VALUE)
            loggers["messages"]["errors"]["logger"].error('...reinited')


def check_tasks():
    try:
        now = int(time.time() * 1000)
        tasks_to_run = []
        try:
            tasks_to_run = vk_tasks.find(
                    {'status': "new", "srcType": "vk", "nextTry": {"$lt": now}}, limit=50)
        except NetworkTimeout:
            tasks_to_run = vk_tasks.find(
                    {'status': "new", "srcType": "vk", "nextTry": {"$lt": now}}, limit=50)

        for row in tasks_to_run:
            try:
                if row["taskType"] != "vkPage":
                    continue
                import_tag = str(uuid4())
                if row["attemptNumber"] < MAX_CHECK_ERRORS*2:
                    row["lastTry"] = now
                    row["nextTry"] = now + CHECK_CHANNELS_QUEUE_PERIOD * 2
                    row["importTag"] = import_tag
                    row["attemptNumber"] += 1
                    queue_name = "channels.vk.custom_2"
                    message = {
                        "importTag": import_tag,
                        "gid": row["taskDetails"]["gid"],
                        "pid": row["taskDetails"]["pid"]
                    }
                    send_message(queue_name, message)
                    loggers["vk"]["info"]["logger"].debug("Created task for page %s of group %s",
                                                          row["taskDetails"]["pid"],
                                                          row["taskDetails"]["gid"])

                else:
                    row["status"] = "error"
                    loggers["vk"]["errors"]["logger"].error("Mark page %s of group %s as error",
                                                            row["taskDetails"]["pid"], row["taskDetails"]["gid"])

                vk_tasks.replace_one({"_id":row["_id"]},row)


            except Exception as err:
                loggers["vk"]["errors"]["logger"].error("Cant process page %s of group %s", row["taskDetails"]["pid"],
                                                        row["taskDetails"]["gid"])
                loggers["vk"]["errors"]["logger"].error(err)
                # yield gen.Task(row.update)


    except Exception as err:
        loggers["vk"]["errors"]["logger"].error("Cant process microtasks")
        loggers["vk"]["errors"]["logger"].error(err)


if __name__ == "__main__":
    loop = ioloop.IOLoop.instance()
    TIME_TO_RUN = 3000

    period_cbk4 = ioloop.PeriodicCallback(check_tasks, 10000, loop)
    period_cbk4.start()


    # period_cbk4 = tornado.ioloop.PeriodicCallback(process_requests.repl_queue, 13000, loop)
    # period_cbk4.start()

    loop.start()
