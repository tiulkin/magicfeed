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
import math
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
client = MongoClient(MONGO_SERVER_IP, socketKeepAlive = True , socketTimeoutMS =30000)
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




# Обработка запросов на добавление каналов
# @gen.coroutine
def check_new():
    try:
        now = int(time.time() * 1000)
        nowText = time.asctime(time.localtime())

        new_docs = requests_db.view('channels/requests', endkey=now, include_docs=True)
        if len(new_docs.rows):
            for row in new_docs.rows:
                try:
                    if int(time.time() * 1000) - now > 3000:
                        break
                    routing_key = None
                    request_doc = requests_db.get(row["id"])
                    db_name = None if "dbName" not in request_doc else request_doc["dbName"]
                    if "nextCheck" not in request_doc:
                        request_doc["nextCheck"] = 0
                    if (request_doc["status"] == "sent" or request_doc["status"] == "checking") and request_doc[
                        "nextCheck"] < now:
                        attempt_number = 1 if "attemptNumber" not in request_doc else request_doc["attemptNumber"] + 1
                        request_doc["attemptNumber"] = attempt_number

                        if attempt_number <= MAX_CHECK_ERRORS * 2:
                            importTag = str(uuid4())
                            message = None
                            row["attemptNnumber"] = attempt_number
                            if row["doc"]["operation"] == "addChannel":
                                srcType = ""
                                loggers[row["doc"]["channelData"]["sourceType"]]["info"]["logger"].debug("add channel %s",
                                                                                                         row["doc"][
                                                                                                             "channelData"][
                                                                                                             "url"])
                                request_doc["lastCheck"] = now
                                request_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * attempt_number
                                try:
                                    result = requests_db.save(request_doc)
                                except:
                                    result = requests_db.save(request_doc)
                                request_doc["lastCheck"] = now
                                request_doc["_rev"] = result[1]
                                if request_doc and "channelData" in request_doc and request_doc["channelData"] and \
                                                "sourceType" in request_doc["channelData"] and request_doc["channelData"][
                                    "sourceType"] and \
                                                "url" in request_doc["channelData"] and request_doc["channelData"]["url"]:
                                    # Посмотрим такой же канал у других
                                    srcType = request_doc["channelData"]["sourceType"]
                                    user_server_id = "azure"
                                    if attempt_number == 3 or attempt_number == 7:
                                        try:
                                            alert = alerts_list.find_one({"_id":request_doc["_id"]})
                                        except NetworkTimeout:
                                            alert = alerts_list.find_one({"_id":request_doc["_id"]})
                                        if not alert:
                                            alert = {
                                                "_id": request_doc["_id"],
                                                "alertType": 2,
                                                "alertTypeTxt": "Сайт, " + (
                                                "три" if attempt_number == 3 else "семь") + " попытки",
                                                "alertLevel": "warning",
                                                "channelId": request_doc["_id"],
                                                "channelName": request_doc["name"],
                                                "channelUrl": request_doc["channelData"]["url"],
                                                "serverId": user_server_id,
                                                "srcType": srcType,
                                                "alertDate": now,
                                                "alertDateTxt": nowText,
                                                "dbName": request_doc["dbName"] if "dbName" in request_doc else "",
                                                "requestDate": request_doc["requestDate"] if "requestDate" in request_doc[
                                                    "channelData"] else now,
                                                "requestDateText": time.asctime(time.localtime(
                                                        request_doc["requestDate"] if "requestDate" in request_doc[
                                                        "channelData"] else now / 1000)),
                                                "status": "new",
                                                "articleAttemptNumber": attempt_number
                                            }
                                        else:
                                            alert["alertType"]= 2
                                            alert["alertTypeTxt"] = "Сайт, " + (
                                            "три" if attempt_number == 3 else "семь") + " попытки",
                                            alert["alertLevel"] = "warning"
                                            alert["alertDate"] = now
                                            alert["alertDateTxt"] = nowText
                                            alert["status"] = "new"
                                        try:
                                            alerts_list.replace_one({"_id": alert["_id"]},alert)
                                        except NetworkTimeout:
                                            alerts_list.replace_one({"_id": alert["_id"]},alert)

                                    try:
                                        channel_to_import = channels_queue.find_one({
                                            'status': 'active',
                                            "channelData.sourceType": row["doc"]["channelData"]["sourceType"],
                                            "channelData.url": row["doc"]["channelData"]["url"]
                                        })
                                    except NetworkTimeout:
                                        channel_to_import = channels_queue.find_one({
                                            'status': 'active',
                                            "channelData.sourceType": row["doc"]["channelData"]["sourceType"],
                                            "channelData.url": row["doc"]["channelData"]["url"]
                                        })

                                    if not channel_to_import:
                                        message = {"_id": request_doc["_id"], "dbName": db_name, "importTag": importTag,
                                                   "mode": "add",
                                                   "userServerId": user_server_id, "serverId": "azure",
                                                   "srcType": srcType}
                                        routing_key = "channels." + request_doc["channelData"]["sourceType"] + ".add"
                                        loggers[row["doc"]["channelData"]["sourceType"]]["info"]["logger"].debug(
                                                "sent message (%s) for channel %s(%s)", importTag, row["doc"]["_id"],
                                                row["doc"]["channelData"]["url"])

                            if routing_key:
                                request_doc["importTag"] = importTag
                                request_doc["lastCheck"] = now
                                request_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * attempt_number
                                loggers["links"]["info"]["logger"].debug("Saving request %s for user %s",
                                                                         request_doc["_id"],
                                                                         db_name)
                                try:
                                    _id, _rev = requests_db.save(request_doc)
                                except:
                                    _id, _rev = requests_db.save(request_doc)
                                request_doc["_rev"] = _rev
                                send_message(routing_key,message)
                            else:
                                request_doc["status"] = "processed"
                                try:
                                    _id, _rev = requests_db.save(request_doc)
                                except:
                                    _id, _rev = requests_db.save(request_doc)
                                request_doc["_rev"] =_rev
                        else:
                            if "srcType" in request_doc:
                                logger_name = request_doc["srcType"]
                            elif ["channelData"] in request_doc and request_doc["channelData"] and ["sourceType"] in \
                                    request_doc["channelData"]:
                                logger_name = request_doc["channelData"]["sourceType"]
                            if logger_name and logger_name in loggers:
                                loggers[logger_name]["info"]["logger"].error("Mark request %s for user %s as error",
                                                                             db_name, request_doc["_id"])
                                loggers[logger_name]["errors"]["logger"].error("Mark request %s for user %s as error",
                                                                               db_name, request_doc["_id"])

                            request_doc["status"] = "error"
                            try:
                                _id, _rev = requests_db.save(request_doc)
                            except:
                                _id, _rev = requests_db.save(request_doc)
                            request_doc["_rev"] = _rev

                            try:
                                alert = alerts_list.find_one({"_id": request_doc["_id"]})
                            except NetworkTimeout:
                                alert = alerts_list.find_one({"_id": request_doc["_id"]})
                            if not alert:
                                alert = {
                                    "_id": request_doc["_id"],
                                    "alertType": 3,
                                    "alertTypeTxt": "Сайт, ошибка",
                                    "alertLevel": "error",
                                    "channelId": request_doc["_id"],
                                    "alertDate": now,
                                    "alertDateTxt": nowText,
                                    "dbName": request_doc["dbName"] if "dbName" in request_doc else "",
                                    "requestDate": request_doc["requestDate"] if "requestDate" in request_doc[
                                        "channelData"] else now,
                                    "requestDateText": time.asctime(time.localtime(
                                            row["doc"]["requestDate"] if "requestDate" in row["doc"][
                                                "channelData"] else now / 1000)),
                                    "status": "new",
                                    "articleAttemptNumber": attempt_number
                                }
                            else:
                                alert["alertType"] = 3
                                alert["alertTypeTxt"] = "Сайт, ошибка",
                                alert["alertLevel"] = "error"
                                alert["alertDate"] = now
                                alert["alertDateTxt"] = nowText
                                alert["status"] = "new"
                            try:
                                alerts_list.replace_one({"_id": alert["_id"]}, alert)
                            except NetworkTimeout:
                                alerts_list.replace_one({"_id": alert["_id"]}, alert)

                except Exception as err:
                    try:
                        if "srcType" in request_doc:
                            logger_name = request_doc["srcType"]
                        elif ["channelData"] in request_doc and request_doc["channelData"] and ["sourceType"] in request_doc[
                            "channelData"]:
                            logger_name = request_doc["channelData"]["sourceType"]
                        if logger_name and logger_name in loggers:
                            loggers[logger_name]["errors"]["logger"].error("Error when checking request %s",
                                                                           request_doc["_id"])
                            loggers[logger_name]["errors"]["logger"].error(err)
                        if  request_doc["operation"] == "connectToChannel":
                            loggers["links"]["info"]["logger"].error("Error when checking request %s", request_doc["_id"])
                            loggers["links"]["errors"]["logger"].error(err)
                    except Exception as err:
                        raise
                    # finally:
                    #     loggers["unknown"]["errors"]["logger"].error(err)

    except couchdb.HTTPError as err:
        loggers["unknown"]["errors"]["logger"].error(err)

    except Exception as err:
        loggers["unknown"]["errors"]["logger"].error(err)


def check_new_en():
    try:
        now = int(time.time() * 1000)
        nowText = time.asctime(time.localtime())

        new_docs = requests_db.view('channels/requests_en', endkey=now, include_docs=True)
        if len(new_docs.rows):
            for row in new_docs.rows:
                try:
                    if int(time.time() * 1000) - now > 3000:
                        break
                    routing_key = None
                    request_doc = requests_db.get(row["id"])
                    db_name = None if "dbName" not in request_doc else request_doc["dbName"]
                    if "nextCheck" not in request_doc:
                        request_doc["nextCheck"] = 0
                    if (request_doc["status"] == "sent" or request_doc["status"] == "checking") and request_doc[
                        "nextCheck"] < now:
                        attempt_number = 1 if "attemptNumber" not in request_doc else request_doc["attemptNumber"] + 1
                        request_doc["attemptNumber"] = attempt_number

                        if attempt_number <= MAX_CHECK_ERRORS * 2:
                            importTag = str(uuid4())
                            message = None
                            row["attemptNnumber"] = attempt_number
                            if row["doc"]["operation"] == "addChannel":
                                srcType = ""
                                loggers[row["doc"]["channelData"]["sourceType"]]["info"]["logger"].debug(
                                    "add channel %s",
                                    row["doc"][
                                        "channelData"][
                                        "url"])
                                request_doc["lastCheck"] = now
                                request_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * attempt_number
                                try:
                                    result = requests_db.save(request_doc)
                                except:
                                    result = requests_db.save(request_doc)
                                request_doc["lastCheck"] = now
                                request_doc["_rev"] = result[1]
                                if request_doc and "channelData" in request_doc and request_doc["channelData"] and \
                                                "sourceType" in request_doc["channelData"] and \
                                        request_doc["channelData"][
                                            "sourceType"] and \
                                                "url" in request_doc["channelData"] and request_doc["channelData"][
                                    "url"]:
                                    # Посмотрим такой же канал у других
                                    srcType = request_doc["channelData"]["sourceType"]
                                    user_server_id = "azure"
                                    if attempt_number == 3 or attempt_number == 7:
                                        try:
                                            alert = alerts_list.find_one({"_id": request_doc["_id"]})
                                        except NetworkTimeout:
                                            alert = alerts_list.find_one({"_id": request_doc["_id"]})
                                        if not alert:
                                            alert = {
                                                "_id": request_doc["_id"],
                                                "alertType": 2,
                                                "alertTypeTxt": "Сайт, " + (
                                                    "три" if attempt_number == 3 else "семь") + " попытки",
                                                "alertLevel": "warning",
                                                "channelId": request_doc["_id"],
                                                "channelName": request_doc["name"],
                                                "channelUrl": request_doc["channelData"]["url"],
                                                "serverId": user_server_id,
                                                "srcType": srcType,
                                                "alertDate": now,
                                                "alertDateTxt": nowText,
                                                "dbName": request_doc["dbName"] if "dbName" in request_doc else "",
                                                "requestDate": request_doc["requestDate"] if "requestDate" in
                                                                                             request_doc[
                                                                                                 "channelData"] else now,
                                                "requestDateText": time.asctime(time.localtime(
                                                        request_doc["requestDate"] if "requestDate" in request_doc[
                                                            "channelData"] else now / 1000)),
                                                "status": "new",
                                                "articleAttemptNumber": attempt_number
                                            }
                                        else:
                                            alert["alertType"] = 2
                                            alert["alertTypeTxt"] = "Сайт, " + (
                                                "три" if attempt_number == 3 else "семь") + " попытки",
                                            alert["alertLevel"] = "warning"
                                            alert["alertDate"] = now
                                            alert["alertDateTxt"] = nowText
                                            alert["status"] = "new"
                                        try:
                                            alerts_list.replace_one({"_id": alert["_id"]}, alert)
                                        except NetworkTimeout:
                                            alerts_list.replace_one({"_id": alert["_id"]}, alert)

                                    try:
                                        channel_to_import = channels_queue.find_one({
                                            'status': 'active',
                                            "channelData.sourceType": row["doc"]["channelData"]["sourceType"],
                                            "channelData.url": row["doc"]["channelData"]["url"]
                                        })
                                    except NetworkTimeout:
                                        channel_to_import = channels_queue.find_one({
                                            'status': 'active',
                                            "channelData.sourceType": row["doc"]["channelData"]["sourceType"],
                                            "channelData.url": row["doc"]["channelData"]["url"]
                                        })

                                    if not channel_to_import:
                                        message = {"_id": request_doc["_id"], "dbName": db_name, "importTag": importTag,
                                                   "mode": "add",
                                                   "userServerId": user_server_id, "serverId": "azure",
                                                   "srcType": srcType}
                                        routing_key = "channels." + request_doc["channelData"]["sourceType"] + ".add"
                                        loggers[row["doc"]["channelData"]["sourceType"]]["info"]["logger"].debug(
                                                "sent message (%s) for channel %s(%s)", importTag, row["doc"]["_id"],
                                                row["doc"]["channelData"]["url"])

                            if routing_key:
                                request_doc["importTag"] = importTag
                                request_doc["lastCheck"] = now
                                request_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * attempt_number
                                loggers["links"]["info"]["logger"].debug("Saving request %s for user %s",
                                                                         request_doc["_id"],
                                                                         db_name)
                                try:
                                    _id, _rev = requests_db.save(request_doc)
                                except:
                                    _id, _rev = requests_db.save(request_doc)
                                request_doc["_rev"] = _rev
                                send_message(routing_key, message)
                            else:
                                request_doc["status"] = "processed"
                                try:
                                    _id, _rev = requests_db.save(request_doc)
                                except:
                                    _id, _rev = requests_db.save(request_doc)
                                request_doc["_rev"] = _rev
                        else:
                            if "srcType" in request_doc:
                                logger_name = request_doc["srcType"]
                            elif ["channelData"] in request_doc and request_doc["channelData"] and ["sourceType"] in \
                                    request_doc["channelData"]:
                                logger_name = request_doc["channelData"]["sourceType"]
                            if logger_name and logger_name in loggers:
                                loggers[logger_name]["info"]["logger"].error("Mark request %s for user %s as error",
                                                                             db_name, request_doc["_id"])
                                loggers[logger_name]["errors"]["logger"].error("Mark request %s for user %s as error",
                                                                               db_name, request_doc["_id"])

                            request_doc["status"] = "error"
                            try:
                                _id, _rev = requests_db.save(request_doc)
                            except:
                                _id, _rev = requests_db.save(request_doc)
                            request_doc["_rev"] = _rev

                            try:
                                alert = alerts_list.find_one({"_id": request_doc["_id"]})
                            except NetworkTimeout:
                                alert = alerts_list.find_one({"_id": request_doc["_id"]})
                            if not alert:
                                alert = {
                                    "_id": request_doc["_id"],
                                    "alertType": 3,
                                    "alertTypeTxt": "Сайт, ошибка",
                                    "alertLevel": "error",
                                    "channelId": request_doc["_id"],
                                    "alertDate": now,
                                    "alertDateTxt": nowText,
                                    "dbName": request_doc["dbName"] if "dbName" in request_doc else "",
                                    "requestDate": request_doc["requestDate"] if "requestDate" in request_doc[
                                        "channelData"] else now,
                                    "requestDateText": time.asctime(time.localtime(
                                            row["doc"]["requestDate"] if "requestDate" in row["doc"][
                                                "channelData"] else now / 1000)),
                                    "status": "new",
                                    "articleAttemptNumber": attempt_number
                                }
                            else:
                                alert["alertType"] = 3
                                alert["alertTypeTxt"] = "Сайт, ошибка",
                                alert["alertLevel"] = "error"
                                alert["alertDate"] = now
                                alert["alertDateTxt"] = nowText
                                alert["status"] = "new"
                            try:
                                alerts_list.replace_one({"_id": alert["_id"]}, alert)
                            except NetworkTimeout:
                                alerts_list.replace_one({"_id": alert["_id"]}, alert)

                except Exception as err:
                    try:
                        if "srcType" in request_doc:
                            logger_name = request_doc["srcType"]
                        elif ["channelData"] in request_doc and request_doc["channelData"] and ["sourceType"] in \
                                request_doc[
                                    "channelData"]:
                            logger_name = request_doc["channelData"]["sourceType"]
                        if logger_name and logger_name in loggers:
                            loggers[logger_name]["errors"]["logger"].error("Error when checking request %s",
                                                                           request_doc["_id"])
                            loggers[logger_name]["errors"]["logger"].error(err)
                        if request_doc["operation"] == "connectToChannel":
                            loggers["links"]["info"]["logger"].error("Error when checking request %s",
                                                                     request_doc["_id"])
                            loggers["links"]["errors"]["logger"].error(err)
                    except Exception as err:
                        raise
                        # finally:
                        #     loggers["unknown"]["errors"]["logger"].error(err)

    except couchdb.HTTPError as err:
        loggers["unknown"]["errors"]["logger"].error(err)

    except Exception as err:
        loggers["unknown"]["errors"]["logger"].error(err)



# Проверка каналов на предмет новых статей и импорт
# @gen.engine
def check_channels_queue():
    try:
        now = int(time.time() * 1000)
        nowText = time.asctime(time.localtime())
        try:
            channels_to_import = channels_queue.find({'status': 'active', "nextCheck": {"$lt": now}})
        except NetworkTimeout:
            channels_to_import = channels_queue.find({'status': 'active', "nextCheck": {"$lt": now}})
        for channel_doc in channels_to_import:
            try:
                # if channel_doc["_id"]!='c_korresponsden_net_all':
                #     continue
                if now - (channel_doc.get("alertPeriod",1000 * 60 * 60 * 24 * 3)) > channel_doc.get("lastImport",0):
                    try:
                        alert = alerts_list.find_one({"_id": channel_doc["_id"]})
                    except NetworkTimeout:
                        alert = alerts_list.find_one({"_id": channel_doc["_id"]})
                    if not alert:
                        alert = {
                            "_id": channel_doc["_id"],
                            "alertType": 5,
                            "alertTypeTxt": "Сайт, давно нет статей",
                            "alertLevel": "error",
                            "channelId": channel_doc["_id"],
                            "channelName": channel_doc.get("name",""),
                            "feedType": channel_doc.get("feedType"),
                            "feedMode": channel_doc.get("feedMode"),
                            "channelUrl": "",
                            "serverId": "",
                            "articleUrl": channel_doc.get("srcUrl"),
                            "srcType": channel_doc.get("srcType"),
                            "alertDate": now,
                            "alertDateTxt": nowText,
                            "status": "new",
                            "channelAttemptNumber": channel_doc.get("attemptNumber",0)
                        }
                    else:
                        alert["alertType"] = 5
                        alert["alertTypeTxt"] = "Сайт, давно нет статей"
                        alert["alertLevel"] = "error"
                        alert["alertDate"] = now
                        alert["alertDateTxt"] = nowText
                        alert["status"] = "new"
                    try:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)
                    except NetworkTimeout:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)

                if channel_doc.get("attemptNumber",0) <= MAX_CHECK_ERRORS*100:
                    channel_doc["attemptNumber"] = channel_doc.get("attemptNumber", 0)+1
                    # channel_doc["attemptNumber"] = 0
                    channel_doc["status"] = "active"
                    channel_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * channel_doc["attemptNumber"]
                    channel_doc["importStatus"] = 'checking'
                    channel_doc["importTag"] = str(uuid4())
                    try:
                        channels_queue.replace_one({"_id": channel_doc["_id"]},channel_doc)
                    except NetworkTimeout:
                        channels_queue.replace_one({"_id": channel_doc["_id"]},channel_doc)
                    queue_name = "channels." + channel_doc["srcType"] + ".process"
                    message = {"importTag": channel_doc["importTag"],"channelId": channel_doc["_id"]}
                    send_message(queue_name,message)
                else:
                    loggers[channel_doc["srcType"]]["errors"]["logger"].error("Mark channel %s as error (new articles)", channel_doc["_id"])
                    channel_doc["status"] = "error"
                    try:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)
                    except NetworkTimeout:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)
                    try:
                        alert = alerts_list.find_one({"_id": channel_doc["_id"]})
                    except NetworkTimeout:
                        alert = alerts_list.find_one({"_id": channel_doc["_id"]})
                    if not alert:
                        alert = {
                            "_id": channel_doc["_id"],
                            "alertType": 6,
                            "alertTypeTxt": "Сайт, ошибка",
                            "alertLevel": "error",
                            "channelId": channel_doc["_id"],
                            "channelName": channel_doc.get("channelName"),
                            "feedType": channel_doc.get("feedType"),
                            "feedMode": channel_doc.get("feedMode"),
                            "channelUrl": "",
                            "serverId": "",
                            "articleUrl": channel_doc.get("srcUrl"),
                            "srcType": channel_doc.get("srcType"),
                            "alertDate": now,
                            "alertDateTxt": nowText,
                            "status": "new",
                            "channelAttemptNumber": channel_doc.get("attemptNumber",0)
                        }
                    else:
                        alert["alertType"] = 6
                        alert["alertTypeTxt"] = "Сайт, ошибка"
                        alert["alertLevel"] = "error"
                        alert["alertDate"] = now
                        alert["alertDateTxt"] = nowText
                        alert["status"] = "new"
                    try:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)
                    except NetworkTimeout:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)
            except Exception as err:
                loggers["unknown"]["errors"]["logger"].error(err)
                loggers[channel_doc["srcType"]]["errors"]["logger"].error("Checking channel %s to import new articles", channel_doc["_id"])

    except Exception as err:
        print(traceback.format_exc())
        print(sys.exc_info()[0])
        loggers["unknown"]["errors"]["logger"].error("Checking channels to import new articles")
        loggers["unknown"]["errors"]["logger"].error(err)


# Продолжение неудачного импорта
# @gen.engine
def process_articles():
    now = int(time.time() * 1000)
    nowText = time.asctime(time.localtime())
    try:
        try:
            articles_to_import = articles_queue.find({"$or": [{'status': 'toImport'}, {'status': 'active'}], "nextTry": {"$lt": now}},sort=[('pubDate', 1),('lastTry', 1)])
        except NetworkTimeout:
            articles_to_import = articles_queue.find({"$or": [{'status': 'toImport'}, {'status': 'active'}], "nextTry": {"$lt": now}},sort=[('pubDate', 1),('lastTry', 1)])
        for article_doc in articles_to_import:
            try:
                if article_doc.get("attemptNumber",0) == 3 or article_doc.get("attemptNumber", 0) == 7:
                    try:
                        alert = alerts_list.find_one({"_id": article_doc["_id"]})
                    except NetworkTimeout:
                        alert = alerts_list.find_one({"_id": article_doc["_id"]})
                    if not alert:
                        alert = {
                            "_id": article_doc["_id"],
                            "alertType": 0,
                            "alertTypeTxt": "Статья, " + ("три" if article_doc.get("attemptNumber", 0) == 3 else "семь") + " попытки",
                            "alertLevel": "warning",
                            "channelId": article_doc.get("channelId"),
                            "channelName": article_doc.get("channelName",""),
                            "feedType": article_doc.get("feedType"),
                            "feedMode": article_doc.get("feedMode"),
                            "channelUrl": "",
                            "serverId": "",
                            "articleId": article_doc["_id"],
                            "articleName": article_doc.get("title",""),
                            "articleUrl": article_doc.get("importUrl"),
                            "srcType": article_doc.get("srcType"),
                            "alertDate": now,
                            "alertDateTxt": nowText,
                            "articlePubDate": article_doc.get("pubDate", 0),
                            "articlePubDateText": time.asctime(time.localtime(article_doc.get("pubDate",0) / 1000)),
                            "status": "new",
                            "articleAttemptNumber": article_doc.get("attemptNumber", 0)
                        }
                    else:
                        alert["alertType"] = 0
                        alert["alertTypeTxt"] = "Статья, " + ("три  попытки" if article_doc.get("attemptNumber", 0) == 3 else "семь  попыток"),
                        alert["alertLevel"] = "warning"
                        alert["alertDate"] = now
                        alert["alertDateTxt"] = nowText
                        alert["status"] = "new"
                    try:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert)
                    except NetworkTimeout:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert)

                if article_doc.get("attemptNumber", 0) <= MAX_CHECK_ERRORS:
                    article_doc["attemptNumber"]=article_doc.get("attemptNumber", 0)+1
                    loggers[article_doc["srcType"]]["info"]["logger"].debug("Attempt %d to import article %s status %s",
                                                                     article_doc["attemptNumber"],
                                                                     article_doc["_id"], article_doc["status"])

                    article_doc["nextTry"] = now + ARTICLE_IMPORT_DELAY *  math.ceil(article_doc["attemptNumber"]/2)
                    article_doc["importTag"] = str(uuid4())
                    try:
                        articles_queue.replace_one({"_id":article_doc["_id"]},article_doc)
                    except NetworkTimeout:
                        articles_queue.replace_one({"_id":article_doc["_id"]},article_doc)
                    queue_name = (
                    "channels." + article_doc["srcType"] + ".items") if article_doc["status"] == "toImport" else "channels.rss.custom_1"
                    message = {"channelId":article_doc.get("channelId"), "importTag": article_doc["importTag"],
                               "articleId": article_doc.get("articleId"), "_id": article_doc.get("_id"),
                               }



                    send_message(queue_name,message)
                else:
                    article_doc["status"] = "error"
                    loggers[article_doc["srcType"]]["errors"]["logger"].error("Mark article %s as error", article_doc["_id"])
                    try:
                        articles_queue.replace_one({"_id": article_doc["_id"]}, article_doc)
                    except NetworkTimeout:
                        articles_queue.replace_one({"_id": article_doc["_id"]}, article_doc)
                    try:
                        alert = alerts_list.find_one({"_id": article_doc["_id"]})
                    except NetworkTimeout:
                        alert = alerts_list.find_one({"_id": article_doc["_id"]})
                    if not alert:
                        alert = {
                            "_id": article_doc["_id"],
                            "alertType": 1,
                            "alertTypeTxt": "Статья, ошибка",
                            "alertLevel": "error",
                            "channelId": article_doc.get("channelId"),
                            "channelName": article_doc.get("channelName"),
                            "feedType": article_doc.get("feedType"),
                            "feedMode": article_doc.get("feedMode"),
                            "channelUrl": "",
                            "serverId": "",
                            "articleId": article_doc.get("_id"),
                            "articleName": article_doc.get("title",""),
                            "articleUrl": article_doc.get("importUrl"),
                            "srcType": article_doc.get("srcType"),
                            "alertDate": now,
                            "alertDateTxt": nowText,
                            "articlePubDate": article_doc.get("pubDate",0),
                            "articlePubDateText": time.asctime(time.localtime(article_doc.get("pubDate",0) / 1000)),
                            "status": "new",
                            "articleAttemptNumber": 10
                        }
                    else:
                        alert["alertType"] = 1
                        alert["alertTypeTxt"] = "Статья, ошибка"
                        alert["alertLevel"] = "error"
                        alert["alertDate"] = now
                        alert["alertDateTxt"] = nowText
                        alert["status"] = "new"
                    try:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)
                    except NetworkTimeout:
                        alerts_list.replace_one({"_id": alert["_id"]}, alert,True)

            except Exception as err:
                loggers[article_doc.get("srcType")]["errors"]["logger"].error(err)
                loggers[article_doc.get("srcType")]["errors"]["logger"].error("Attempt %d to import article %s, status %s",
                                                                              article_doc.get("attemptNumber",0),
                                                                              article_doc.get("_id"), article_doc.get("status"))
                print(traceback.format_exc())
                print(sys.exc_info()[0])
    except Exception as err:
        loggers["unknown"]["errors"]["logger"].error("Force article to import")
        loggers["unknown"]["errors"]["logger"].error(err)


# Обработка каналов, у которых не закончилась проверка
# @gen.engine
def error_channels_queue():
    try:
        now = int(time.time() * 1000)
        try:
            channels_to_import = channels_queue.find({'status': 'checking', "nextCheck": {"$lt": now}},sort=[("lastCheck", 1)])
        except NetworkTimeout:
            channels_to_import = channels_queue.find({'status': 'checking', "nextCheck": {"$lt": now}},sort=[("lastCheck", 1)])
        for channel_doc in channels_to_import:
            try:
                channel_doc["attemptNumber"] = channel_doc.get("attemptNumber", 0) + 1
                channel_doc["lastCheck"] = now
                channel_doc["nextCheck"] = now + CHECK_CHANNELS_QUEUE_PERIOD * channel_doc.get("attemptNumber", 0)
                channel_doc["importTag"] = str(uuid4())

                if channel_doc["attemptNumber"] <= MAX_CHECK_ERRORS:
                    channel_doc["importStatus"] = 'checking'
                    try:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)
                    except NetworkTimeout:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)
                    queue_name = "channels." + channel_doc["srcType"] + ".add"
                    db_name = channel_doc["dbName"]
                    server_id = channel_doc["serverId"]
                    message = {"_id": channel_doc["requestId"], "srcType": channel_doc["srcType"], "mode": "tryAgain", "dbName": db_name,
                               "serverId": server_id, "importTag": channel_doc["importTag"]}
                    send_message(queue_name,message)

                    # if channel_doc["attemptNumber"] > 1 and channel_doc["attemptNumber"] < (MAX_CHECK_ERRORS - 1):
                    #     routing_key = "channels.link.update"
                    #     message = {"_id": channel_doc["_id"], "dbName": db_name,
                    #                "creatorServerId": channel_doc.get("creatorServerId","azure"),
                    #                "srcType": channel_doc["srcType"], "attemptNumber": channel_doc["attemptNumber"]}
                    #     send_message(routing_key,message)
                else:
                    loggers[channel_doc["srcType"]]["errors"]["logger"].error("Mark channel %s(%s) as error when checking",
                                                                   channel_doc["_id"], channel_doc["srcUrl"])
                    loggers[channel_doc["srcType"]]["errors"]["logger"].error("Force marking channel %s(%s) as error in mongodb",
                                                                   channel_doc["_id"], channel_doc["srcUrl"])
                    channel_doc["status"] = 'error'
                    try:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)
                    except NetworkTimeout:
                        channels_queue.replace_one({"_id": channel_doc["_id"]}, channel_doc)

            except Exception as err:
                loggers[channel_doc["srcType"]]["errors"]["logger"].error(err)
                loggers[channel_doc["srcType"]]["errors"]["logger"].error("Attempt %d to check channel %s",
                                                               channel_doc["attemptNumber"], channel_doc["_id"])
                print(traceback.format_exc())
                print(sys.exc_info()[0])

    except Exception as err:
        loggers["unknown"]["errors"]["logger"].error("Force channel to recheck")
        loggers["unknown"]["errors"]["logger"].error(err)


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
                if row["attemptNumber"] < MAX_CHECK_ERRORS:
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
                    row.status = "error"
                    loggers["vk"]["errors"]["logger"].error("Mark page %s of group %s as error",
                                                            row["taskDetails"]["pid"], row["taskDetails"]["gid"])

                vk_tasks.replace_one({"_id":row["_id"]},row)


            except Exception as err:
                loggers["vk"]["errors"]["logger"].error("Cant process page %s of group %s", row.taskDetails["pid"],
                                                        row["taskDetails"]["gid"])
                loggers["vk"]["errors"]["logger"].error(err)
                # yield gen.Task(row.update)


    except Exception as err:
        loggers["vk"]["errors"]["logger"].error("Cant process microtasks")
        loggers["vk"]["errors"]["logger"].error(err)

# Обработка каналов, у которых не закончилась проверка
# @gen.engine
def check_putin():
    try:
        now = int(time.time() * 1000)
        queue_name = "channels.putin.process"
        message = {"importTag": "putin", "channelId": "c_putin"}
        send_message(queue_name,message)

    except Exception as err:
        loggers["putin"]["errors"]["logger"].error(err)
        print(traceback.format_exc())
        print(sys.exc_info()[0])



if __name__ == "__main__":
    loop = ioloop.IOLoop.instance()
    TIME_TO_RUN = 3000
    period_cbk = ioloop.PeriodicCallback(check_new, 10000, loop)
    period_cbk.start()

    period_cbk1 = ioloop.PeriodicCallback(check_channels_queue, 10000, loop)
    period_cbk1.start()

    period_cbk2 = ioloop.PeriodicCallback(process_articles, 30000, loop)
    period_cbk2.start()

    period_cbk3 = ioloop.PeriodicCallback(error_channels_queue, 15000, loop)
    period_cbk3.start()

    period_cbk4 = ioloop.PeriodicCallback(check_new_en, 11000, loop)
    period_cbk4.start()

    period_cbk5 = ioloop.PeriodicCallback(check_putin, 30000, loop)
    period_cbk5.start()





    # period_cbk4 = tornado.ioloop.PeriodicCallback(process_requests.repl_queue, 13000, loop)
    # period_cbk4.start()

    loop.start()
