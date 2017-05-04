__author__ = 'oleg'
# !/usr/bin/env python3
from tornado import gen
import io
import couchdb
import requests
from tornado.escape import json_decode, json_encode, url_escape
from dateutil.parser import *
from dateutil.tz import *
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from uuid import uuid4
from tornado.ioloop import IOLoop
from sys import getsizeof
# import pymongo
from pymongo import *
from pymongo.errors import NetworkTimeout
import abc
import urllib
import time
# import couch
import tldextract
import logging
import traceback
from constants import *
import os
import tracemalloc
import hashlib
import random
proxies ={}
last_import=0
current_proxy=0


tracemalloc.start()
from loggers import loggers
from tornado import httpclient
# httpclient.AsyncHTTPClient.configure("curl_httpclient_leaks_patched.CurlAsyncHTTPClientEx")

connection = None
channel = None

from base_consumer import BaseConsumer
from couchdb import http

target_server_url = "http://" + COUCH_USERS_CREDS["creds"] + "@" + COUCH_SERVERS["azure"]["host"]
internal_server_url = "http://" + COUCH_USERS_CREDS["creds"] + "@" + COUCH_SERVERS["azure_internal"]["host"]

client = MongoClient(MONGO_SERVER_IP,socketKeepAlive=True, socketTimeoutMS=30000)
db = client.mf
channels_queue = db.ChannelsQueue
articles_queue = db.ArticlesImportLog
stories_queue = db.Stories
vk_tasks = db.CustomWork



server = couchdb.client.Server(internal_server_url,session=http.Session(timeout=60))
internal_server = couchdb.client.Server(internal_server_url, session=http.Session(timeout=60))


def url_fix(s, charset='utf-8'):
    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(s)
    idn_netloc = netloc.encode('idna').decode('utf-8')
    path = urllib.parse.quote(path, '/%:#-')
    qs = urllib.parse.quote_plus(qs, ':&=#-')
    return urllib.parse.urlunsplit((scheme, idn_netloc, path, qs, anchor))

def copy_channel_to_mobile(db_name):
    try:
        tmpdb = server[db_name + "_m"]
    except couchdb.http.ResourceNotFound:
        tmpdb = server.create(db_name + "_m")
        tmpdb.save({
            "_id": "_design/articles",
            "views": {
                "articles": {
                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                }
            }
        })
    except:
        tmpdb = server[db_name + "_m"]
    try:
        tmpdb = server[db_name + "_50"]
    except couchdb.http.ResourceNotFound:
        tmpdb = server.create(db_name + "_50")
        tmpdb.save({
            "_id": "_design/articles",
            "views": {
                "articles": {
                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                }
            }
        })
    except:
        tmpdb = server[db_name + "_50"]

    try:
        tmpdb = server[db_name + "_m_50"]
    except couchdb.http.ResourceNotFound:
        tmpdb = server.create(db_name + "_m_50")
        tmpdb.save({
            "_id": "_design/articles",
            "views": {
                "articles": {
                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                }
            }
        })
    except Exception:
        tmpdb = server.create(db_name + "_m_50")
    #internal_server.replicate(target_server_url + '/' + db_name,target_server_url + '/' + db_name + "_m",continuous=False, create_target=True, doc_ids=[db_name])
    try:
        internal_server.replicate(db_name,db_name + "_m",continuous=False, create_target=True, doc_ids=[db_name])
    except Exception:
        internal_server.replicate(db_name,db_name + "_m", continuous=False, create_target=True, doc_ids=[db_name])

    try:
        internal_server.replicate(db_name,db_name + "_50",continuous=False, create_target=True, doc_ids=[db_name])
    except Exception:
        internal_server.replicate(db_name, db_name + "_50", continuous=False, create_target=True, doc_ids=[db_name])
    try:
        internal_server.replicate(db_name,db_name + "_m_50",continuous=False, create_target=True, doc_ids=[db_name])
    except Exception:
        internal_server.replicate(db_name,db_name + "_m_50",continuous=False, create_target=True, doc_ids=[db_name])


class AddChannel(object):
    def __init__(self, message, consumer):

        self._consumer = consumer
        self._channel_type = consumer.channel_type
        self._client = consumer.http_client
        self._queue_type = consumer.queue_type
        self._client_config = CLIENT_CONFIG
        try:
            self._requests_db = server[COUCH_REQUESTS_PRIVATE["database"]]
        except:
            self._requests_db = server[COUCH_REQUESTS_PRIVATE["database"]]
        self._channel_record = None
        self._channel_doc = None
        self._request_doc = None
        self._channel_record = None
        self._db_name = None
        self._message = None
        self._channels_db = None
        self._mode = None
        try:
            self._message= message
            self._db_name = self._message.get("dbName")

            self._mode = self._message.get("mode")


            if self._mode == "tryAgain":
                try:
                    self._channel_record = channels_queue.find_one({"importTag":self._message.get("importTag")})
                except NetworkTimeout:
                    self._channel_record = channels_queue.find_one({"importTag": self._message.get("importTag")})
                if self._channel_record and self._channel_record != "active":
                    try:
                        self._request_doc = self._requests_db.get(self._channel_record["requestId"])
                    except:
                        self._request_doc = self._requests_db.get(self._channel_record["requestId"])
            else:
                try:
                    self._request_doc = self._requests_db.get(self._message["_id"])
                except:
                    self._request_doc = self._requests_db.get(self._message["_id"])
                if self._request_doc["importTag"] != self._message["importTag"]:
                    return
            if not self._request_doc:
                return

            self.locale_ext = self._request_doc.get("locale") + '_' if self._request_doc.get("locale") else ""
            if self._db_name:
                try:
                    self._channels_db = server['c_' + self._message.get("_id")]
                except couchdb.http.ResourceNotFound:
                    self._channels_db = server.create('c_' + self._message.get("_id"))
                except:
                    try:
                        self._channels_db = server['c_' +  self._message.get("_id")]
                    except couchdb.http.ResourceNotFound:
                        self._channels_db = server.create('c_' +  self._message.get("_id"))
            else:
                try:
                    self._channels_db = server[ COUCH_CHANNELS_LIST_PRIVATE["database"+self._request_doc.get("locale","")]]
                except:
                    self._channels_db = server[ COUCH_CHANNELS_LIST_PRIVATE["database"+self._request_doc.get("locale","")]]
            self.check_channel()

        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",self._message["_id"], self._message["importTag"], self._mode)
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise


    def get_url(self):
        try:
            if "srcUrl" in self._request_doc["channelData"]:
                href = self._request_doc["channelData"]["srcUrl"]
            else:
                # if self._request_doc["channelData"]["sourceType"]=="rss":
                #     href = self._request_doc["channelData"]["url"]
                href = CHANNEL_TYPE_URL_PARTS[self._request_doc["channelData"]["sourceType"]]["urlPrefix"] + \
                       self._request_doc["channelData"]["url"] + \
                       CHANNEL_TYPE_URL_PARTS[self._request_doc["channelData"]["sourceType"]]["urlSuffix"]

            href = href.replace('feed://', 'http://')
            tldextract.extract(href)
            url = urllib.parse.urlparse(href)
            return href
        except Exception as err:
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(
                "Check url %s for request %s",
                self._request_doc["channelData"]["url"], self._request_doc["_id"])
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(err)
            return None

    def commit_error(self):
        try:
            self._request_doc["errorStage"] = "first"
            self._request_doc["status"] = "processed"
            self._channel_record = None
            try:
                _id,_rev = self._requests_db.save(self._request_doc)
                self._request_doc["_rev"] = _rev
            except:
                _id, _rev = self._requests_db.save(self._request_doc)
                self._request_doc["_rev"] = _rev

            loggers[self._request_doc["channelData"]["sourceType"]]["info"]["info"].error(
                    "Commit error for request %s", self._request_doc["_id"])

        except Exception as err:
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(
                    "Commit error for request %s", self._request_doc["_id"])
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(err)
            raise

    def get_channel_couch(self, srcUrl, extra_keys=None):
        channel_doc=None
        try:
            channel_doc = self._channels_db.get("c_" +  self._request_doc["_id"])
        except:
            channel_doc = self._channels_db.get("c_" +  self._request_doc["_id"])
        if not channel_doc:
            try:
                channel_doc = self._request_doc.copy()
                channel_doc["_id"] = "c_" + channel_doc["_id"]

                del channel_doc["_rev"]
                if "forcedAccess" in channel_doc.keys():
                    access_mode = channel_doc["forcedAccess"]
                else:
                    access_mode = "private"

                if "dbName" in channel_doc and len(channel_doc["dbName"]) > 0:
                    # Можно будет добавить проверку существования БД
                    channel_doc["creatorServerId"] = channel_doc["serverId"]
                    self._db_name = channel_doc["dbName"]
                    channel_doc["subscribersCount"] = 1
                else:
                    self._db_name = None
                    channel_doc["subscribersCount"] = 0

                scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(srcUrl)
                if self._channel_type == "lj":
                    channel_doc["name"] = "LJ – " + channel_doc["name"]
                elif self._channel_type == "vk":
                    channel_doc["name"] = "VK – " + channel_doc["name"]
                else:
                    scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(srcUrl)
                    channel_doc["name"] = netloc + " – " + channel_doc["name"]
                if "description" not in channel_doc:
                    channel_doc["description"] = ""

                channel_doc["serverId"] = DEFAULT_CHANNELS_SERVER
                channel_doc["status"] = "checking"
                channel_doc["accessMode"] = access_mode
                channel_doc["requestId"] = channel_doc["_id"]
                channel_doc["creatorId"] = self._db_name
                channel_doc["type"] = "channel"
                channel_doc["srcUrl"] = srcUrl
                channel_doc["srcType"] = self._channel_type
                if extra_keys:
                    channel_doc = dict(list(channel_doc.items()) + list(extra_keys.items()))
                try:
                    id, rev = self._channels_db.save(channel_doc)
                except:
                    id, rev = self._channels_db.save(channel_doc)

                channel_doc["_rev"] = rev
                if self._db_name and (self._channel_type == "rss" or self._channel_type == "lj"):
                    copy_channel_to_mobile(channel_doc["_id"])
            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].error(
                        "Saving channel %s to couch", self._request_doc["_id"])
                loggers[self._channel_type]["errors"]["logger"].error(err)
                raise
        return channel_doc

    def add_channel_mongo(self, channel_doc, src_attrs=None, extra_keys=None):
        try:
            doc_data = {
                "_id": channel_doc["_id"],
                "requestId": self._request_doc["_id"],
                "name": channel_doc["name"],
                "feedMode": channel_doc["feedMode"] if channel_doc.get("feedMode") else "web",
                "dbName": self._db_name,
                "locale": self._request_doc.get("locale"),
                "serverId": channel_doc["serverId"],
                "creatorServerId": "" if "creatorServerId" not in channel_doc else channel_doc["creatorServerId"],
                "status": "checking",
                "state": "checkPage",
                "accessMode": channel_doc["accessMode"],
                "channelCategories": [] if "categories" not in channel_doc else channel_doc["categories"],
                "srcType": self._channel_type,
                "channelData": self._request_doc["channelData"],
                "srcUrl": channel_doc["srcUrl"],
                "srcUrlPublic": channel_doc["srcUrl"] if "srcUrlPublic" not in channel_doc else channel_doc[
                    "srcUrlPublic"],
                "image": {},
                "description": channel_doc["description"],
                "lastCheck": int(time.time() * 1000),
                "nextCheck": int(time.time() * 1000 + ARTICLE_IMPORT_DELAY),
                "attemptNumber": 0

            }
            if self._request_doc["channelData"].get("notYahoo"):
                doc_data["notYahoo"]=True
            if src_attrs:
                doc_data["srcAttrs"] = src_attrs
            if extra_keys:
                doc_data = dict(list(doc_data.items()) + list(extra_keys.items()))
            try:
                channels_queue.replace_one({"_id": channel_doc["_id"]},doc_data,True)
            except NetworkTimeout:
                channels_queue.replace_one({"_id": channel_doc["_id"]},doc_data,True)

        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Saving channel %s (%s),user %s to mongo",
                                                                  channel_doc["_id"], channel_doc["name"],
                                                                  self._db_name)
            loggers[self._channel_type]["errors"]["logger"].error(channel_doc)

            loggers[self._channel_type]["errors"]["logger"].error(err)
            doc_data = None
        return doc_data

    def commit_request(self):
        try:
            try:
                self._request_doc = self._requests_db.get(self._request_doc["_id"])
            except:
                self._request_doc = self._requests_db.get(self._request_doc["_id"])
            if not self._request_doc:
                self._request_doc["status"] = "error"
                self._request_doc["description"] = "missing record in user's db"
                return False
            else:
                self._request_doc["status"] = "processed"
                try:
                    _id, _rev = self._requests_db.save(self._request_doc)
                except:
                    _id, _rev = self._requests_db.save(self._request_doc)
                self._request_doc["_rev"] = _rev
            return True
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Commit request %s ,user %s",
                                                                  self._request_doc["_id"], self._db_name)
            loggers[self._channel_type]["errors"]["logger"].error(err)
            return False

    @abc.abstractmethod
    def get_update_data(self, body):
        return

    def commit_channel(self):
        try:
            try:
                self._channel_doc = self._channels_db.get('c_' +  self._request_doc["_id"])
            except:
                self._channel_doc = self._channels_db.get('c_' +  self._request_doc["_id"])
            self._channel_doc["status"] = "active"
            try:
                self._channel_record = channels_queue.find_one({"_id": 'c_' +  self._request_doc["_id"]})
            except NetworkTimeout:
                self._channel_record = channels_queue.find_one({"_id": 'c_' + self._request_doc["_id"]})
            self._channel_record["status"] = "active"
            self._channel_record["state"] = "done"
            self._channel_record["nextCheck"] = int(time.time() * 1000 + ARTICLE_IMPORT_DELAY)
            self._channel_record["importTag"] = str(uuid4())

            try:
                _id, _rev = self._channels_db.save(self._channel_doc)
            except Exception as err:
                _id, _rev = self._channels_db.save(self._channel_doc)
            self._channel_doc["_rev"] = _rev
            if self._db_name and (self._channel_type == "rss" or self._channel_type == "lj"):
                copy_channel_to_mobile('c_' + self._request_doc["_id"])

            try:
                channels_queue.replace_one({"_id":self._channel_record["_id"]},self._channel_record)
            except NetworkTimeout:
                channels_queue.replace_one({"_id":self._channel_record["_id"]},self._channel_record)
            return True
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Commit channel %s (%s) ,user %s",
                                                                  self._channel_record["_id"],
                                                                  self._channel_record["originalName"], self._db_name)
            loggers[self._channel_type]["errors"]["logger"].error(err)
            return False

    def add_channel(self, response):
        try:
            channel_id = "c_" + self._request_doc["_id"]
            try:
                self._channel_record = channels_queue.find_one({"_id":channel_id})
            except NetworkTimeout:
                self._channel_record = channels_queue.find_one({"_id":channel_id})
            if self._channel_record and self._channel_record["status"]== "checking":
                try:
                    self._channel_doc = self._channels_db.get(channel_id)
                except:
                    self._channel_doc = self._channels_db.get(channel_id)
                if self._channel_doc:
                    self._channel_record["lastCheck"] = int(time.time() * 1000)
                    responseBody = response.body
                    # channelData = None
                    channelData = None if not self._request_doc.get("channelData") else self._request_doc["channelData"]
                    if response.error:
                        loggers[self._channel_type]["errors"]["logger"].error(
                            "Error in response for channel %s (%s), url %s, user %s",
                            "c_" + self.locale_ext + self._request_doc["_id"],
                            self._channel_record["name"] or self._channel_record["originalName"],
                            self._channel_record["srcUrl"], self._db_name)
                        if self._request_doc and "name" in self._request_doc["channelData"] and "description" in \
                                self._request_doc["channelData"]:
                            channelData = self._request_doc["channelData"]
                        else:
                            return
                    update_data = self.get_update_data(responseBody, channelData)
                    success = self.commit_channel()
                    if not success:
                        return
                    time.sleep(1)
                    queue_name = "channels." + self._channel_type + ".process"
                    message = {"importTag": self._channel_record["importTag"],"channelId":channel_id}
                    self._consumer.send_message(queue_name, message)

                    if update_data:
                        queue_name = "channels." + self._channel_type + ".update"
                        update_data["_id"] = self._channel_record["_id"]
                        update_data["dbName"] = self._channel_record.get("dbName","")
                        if self._channel_record.get("locale"):
                            update_data["dbName"] = "channels"+self._channel_record.get("locale")
                        update_data["serverId"] = self._channel_record.get("creatorServerId","")
                        self._consumer.send_message(queue_name, update_data)
                else:
                    loggers[self._channel_type]["info"]["logger"].debug("Channel record %s not found in couch",
                                                                        "c_" + self.locale_ext + self._request_doc["_id"])
            else:
                loggers[self._channel_type]["info"]["logger"].debug("Channel record %s not found in mongo",
                                                                    "c_" + self.locale_ext + self._request_doc["_id"])


        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Add channel %s ,user %s",
                                                                  "c_" + self._request_doc["_id"], self._db_name)
            loggers[self._channel_type]["errors"]["logger"].error(err)

    def check_channel(self, body=None):
        try:
            if self._mode == "add":
                href = self.get_url()
                if href:
                    channel_doc = self.get_channel_couch(href)
                else:
                    self.commit_error()
                    return
                new_mongo_channel = self.add_channel_mongo(channel_doc)
                success = self.commit_request()
                if not success or not new_mongo_channel:
                    return

            else:
                href = self._channel_record["srcUrl"]
                if href[0:3]!="http": href="http://"+ href

            response = self._client.fetch(href)
            #
            self.add_channel(response)


        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Check channel")
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise


class ProcessChannelImport(object):

    def __init__(self,message,consumer):
        global last_import
        self._consumer = consumer
        self._attemptNumber = 0
        self._channel_type = consumer.channel_type
        self._client = consumer.http_client

        try:
            self._async_client = consumer.asynch_http_client
        except:
            self._async_client = None
        self._queue_type = consumer.queue_type
        self._message= message
        self._url = None
        self._body = json_decode(message.body.decode('utf_8'))
        self._channel_record = None
        proxy=""
        try:
            channel_task=None
            try:
                self._channel_record = channels_queue.find_one({"importTag": self._body["importTag"]})
            except NetworkTimeout:
                self._channel_record = channels_queue.find_one({"importTag": self._body["importTag"]})
            if self._channel_record:
                headers = None
                if self._channel_record.get("listCookies"):
                    headers = {'cookie': self._channel_record.get("listCookies")}

                if self._channel_type == 'rss' or self._channel_type == 'lj':
                    response = None
                    feedType = self._channel_record.get("feedType","rss")
                    if not feedType:
                        try:
                            if headers:
                                response = self._client.fetch(self._url, headers={'cookie': self._channel_record.get("listCookies")})
                            else:
                                response = self._client.fetch(self._url)
                        except Exception as err:
                            try:
                                loggers[self._channel_type]["errors"]["logger"].error("Trying to detect feed type for %s.",self._url)
                                loggers[self._channel_type]["errors"]["logger"].error(err)
                                time.sleep(2)
                                response = self._client.fetch(self._url)
                            except Exception as err:
                                try:
                                    loggers[self._channel_type]["errors"]["logger"].error(
                                        "Trying to detect feed type for %s. Again.", self._url)
                                    loggers[self._channel_type]["errors"]["logger"].error(err)
                                    time.sleep(2)
                                    response = self._client.fetch(self._url)
                                except Exception as err:
                                    loggers[self._channel_type]["errors"]["logger"].error("Can't detect feed type for %s.", self._url)
                                    loggers[self._channel_type]["errors"]["logger"].error(err)

                        if response and response.body and not response.error:
                            soup = BeautifulSoup(response.body, features="xml")
                            if hasattr(soup, 'channel') and soup.channel:
                                feedType = "rss"
                            elif hasattr(soup, 'feed') and soup.feed:
                                feedType = "atom"
                            channels_queue.replace_one({"_id": self._channel_record["_id"]}, self._channel_record)

                    self._url = self._channel_record.get("srcUrl") if self._channel_record.get("notYahoo") else 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20' + feedType + '%20where%20url=%22' + self._channel_record.get("srcUrl") + '%22%20limit%2015&format=json'
                else:
                    self._url = self._channel_record.get("srcUrl")

                if hasattr(self._client,"proxies"):
                    proxy=self.get_proxy()
                    if not proxy:
                        loggers[self._channel_type]["errors"]["logger"].error("Empty proxy")
                        self._message.delete()
                        return

                if headers:
                    # self._async_client.fetch(self._url,callback=self.process_import_url,proxy_host=proxy[0],proxy_port=int(proxy[1]) if proxy[1] else None,
                    #                               headers={'cookie': self._channel_record.get("listCookies")})
                    cookies= {
                        "yandexuid":"3462061651460009242",
                        "path":"/",
                        "domain":".yandex.ru",
                        "expires":"Thu, 31-Dec-2037 20:59:59 GMT",
                        "mynews":"0%3A1",
                        "domain":"news.yandex.ru",
                        "path":"/",
                        "expires":"Wed, 06-Jul-2016 06:07:22 GMT"
                    }
                    # self._url="http://korrespondent.net"
                    response = requests.get(self._url, cookies=cookies, timeout = 30,proxies={"http"  : "http://"+proxy,"https" :"https://"+ proxy })

                    # self._async_client.fetch("http://google.com",callback=self.process_import_url)

                else:
                    response = self._client.fetch(self._url)
                self.process_import_url(response)
                last_import=time.time()


            else:
                self._message.delete()
                return
        except Exception as err:
            # if self._client.proxies:
            #     self._client.proxies.remove(proxy)
            if self._channel_record:
                loggers[self._channel_type]["errors"]["logger"].error("Trying to get channel record for tag %s.",
                                                                      self._body["importTag"])
                loggers[self._channel_type]["errors"]["logger"].error(err)
            else:
                loggers["unknown"]["errors"]["logger"].error("Trying to get channel record")
                loggers["unknown"]["errors"]["logger"].error(err)
            raise


    def get_proxy(self):
        global last_import
        global current_proxy
        global proxies
        login = "RUS180310"
        password = "SGea629MXr"
        result = None
        if time.time()-last_import<60: return None
        if not proxies:
            for proxy in self._client.proxies:
                proxies[proxy]=0
            # result=login+":"+password+"@"+self._client.proxies[0]
            result=self._client.proxies[0]
            proxies[proxy]=time.time()
        else:
            while not result and current_proxy<len(self._client.proxies):
                current_proxy+=1
                if time.time()-proxies[self._client.proxies[current_proxy]]>30*30:
                    # result=login+":"+password+"@"+self._client.proxies[self.current_proxy]
                    result=self._client.proxies[current_proxy]
                    proxies[self._client.proxies[current_proxy]]=time.time()
        if not result:
            current_proxy=0
        return result


    def commit_import(self, isImported):
        try:
            # self._channel_record = channels_queue.get(self._channel_record["_id"])
            self._channel_record["nextCheck"] = int(time.time() * 1000 + ARTICLE_IMPORT_DELAY)
            self._channel_record["attemptNumber"] = 0
            try:
                channels_queue.replace_one({"_id":self._channel_record["_id"]}, self._channel_record)
            except NetworkTimeout:
                channels_queue.replace_one({"_id":self._channel_record["_id"]}, self._channel_record)
            self._message.delete()
            if isImported:
                loggers[self._channel_type]["info"]["logger"].debug("Commited import of the channel %s (%s)",
                                                                    self._channel_record["name"] or self._channel_record["originalName"],
                                                                    self._channel_record["_id"])
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error(
                    "Error while commit the import of the channel %s (%s)",
                    self._channel_record["name"] or self._channel_record["originalName"], self._channel_record["_id"])
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise

    def add_article_record(self, article_url, article_id, channel_id, server_id, channel_name, channel_original_name,
                           channel_categories, title, pub_date,
                           body=None, images=None, articleAttrs=None, feed_type=None, feed_mode=None, viewportWidth=0):
        article_md5 = hashlib.md5((channel_id + "_" + article_url).encode()).hexdigest()

        try:
            if not channel_name:
                channel_name = channel_original_name
            article_import_record = {
                "_id": article_md5,
                "importUrl": article_url,
                "importTag": str(uuid4()),
                "channelId": channel_id,
                "serverId": server_id,
                "articleId": article_id,
                "status": "toImport",
                "channelOriginalName": channel_original_name,
                "channelName": channel_name,
                "channelCategories": channel_categories,
                "srcType": self._channel_type,
                "title": title,
                "pubDate": pub_date,
                "body": body,
                "images": images,
                "articleAttrs": articleAttrs,
                "errorsCount": 0,
                "feedType": feed_type,
                "feedMode": feed_mode,
                "viewportWidth": viewportWidth if viewportWidth else 1152,
                "lastTry": time.time() * 1000,
                "nextTry": time.time() * 1000 +  ARTICLE_IMPORT_DELAY,
                "disableSW": self._channel_record.get("disableSW"),
                "mobileMode": self._channel_record.get("mobileMode"),
                "cookies": self._channel_record.get("cookies")
            }
            try:
                articles_queue.replace_one({"_id": article_md5},article_import_record,True)
            except NetworkTimeout:
                articles_queue.replace_one({"_id": article_md5},article_import_record,True)

            loggers[self._channel_type]["info"]["logger"].debug(
                    "Added mongodb record for the article %s,url %s, channel %s (%s)",
                    article_md5, article_url, channel_name, self._channel_record["_id"])


        except Exception as err:
            loggers[self._channel_type]["info"]["logger"].error(
                    "Trying to add mongodb record for the article %s,url %s, channel %s (%s)",
                    article_md5, article_url, channel_name, self._channel_record["_id"])
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise
        else:
            return article_import_record

    @abc.abstractmethod
    def process_response(self, response):
        # returns list with links to articles for import updates channel's data
        return

    def get_article_attrs(self, article_id, item):
        return {}


    def process_import_url(self, response):
        items = []
        try:
            if response and response.body and not response.error:
                items = self.process_response(response)
            else:
                if self._attemptNumber <= 5:
                    self._attemptNumber += 1
                    response = self._client.fetch(self._url)
                    self.process_import_url(response)
                else:
                    self._message.delete()
                    loggers[self._channel_type]["errors"]["logger"].error(
                            "Empty response for the channel %s (%s)",
                            self._channel_record.get("name") or self._channel_record.get("originalName"),self._channel_record.get("_id"))


        except Exception as err:
            self._message.unlock()
            loggers[self._channel_type]["errors"]["logger"].error(
                    "Processing response fo the channel %s (%s)",
                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                    self._channel_record.get("_id"))
            loggers[self._channel_type]["errors"]["logger"].error(err)
        else:
            need_clean = True
            isImported = False
            for index in range(len(items)):
                item = items[index]
                if item["pubDate"] > int(time.time() * 1000) + ARTICLE_IMPORT_DELAY:
                    continue
                if self._channel_record.get("srcUrl") == "http://lenta.ru/rss" and item["link"][:8] == "http:///":
                    article_url = item["link"].replace("http:///", "http://lenta.ru/")
                else:
                    article_url = item["link"]
                article_md5 = hashlib.md5((self._channel_record.get("_id") + "_" + article_url).encode()).hexdigest()
                try:
                    article_import_record = articles_queue.find_one({"_id": article_md5})
                except NetworkTimeout:
                    article_import_record = articles_queue.find_one({"_id": article_md5})
                if not article_import_record:
                    try:
                        isImported = True
                        channel_to_clean = self._channel_record.get("_id")
                        if need_clean:
                            need_clean = False
                            queue_name = "channels.clean.custom_1"
                            message = {"channelId": channel_to_clean, "serverId": self._channel_record.get("serverId"),
                                       "limit": None}

                            self._consumer.send_message(queue_name, message)
                            loggers[self._channel_type]["info"]["logger"].debug(
                                    "Sent message to cleanip channel %s (%s)",
                                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                                    self._channel_record.get("_id")
                            )
                            loggers["cleanup"]["info"]["logger"].debug(
                                    "Sent message to clean channel %s (%s)",
                                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                                    self._channel_record.get("_id")
                            )

                        article_id = str(uuid4())
                        artilcle_info = self.get_article_attrs(article_id, item)
                        body = "" if "body" not in artilcle_info else artilcle_info["body"]
                        images = [] if "images" not in artilcle_info else artilcle_info["images"]
                        articleAttrs = {} if "articleAttrs" not in artilcle_info else artilcle_info["articleAttrs"]

                        article_import_record = self.add_article_record(article_url,
                            article_id,
                            self._channel_record.get("_id"),
                            self._channel_record.get("serverId"),
                            self._channel_record.get("name"),
                            self._channel_record.get("originalName"),
                            self._channel_record.get("channelCategories"),
                            item["title"],
                            item["pubDate"],
                            body,
                            images, articleAttrs,
                            self._channel_record.get("feedType"),
                            self._channel_record.get("feedMode"),
                            self._channel_record.get("viewportWidth"))


                    except Exception as err:
                        loggers[self._channel_type]["errors"]["logger"].error(
                                "Creation mongo record for the article %s (%s), channel %s (%s)",
                                article_import_record.get("_id"), article_url,
                                self._channel_record.get("name") or self._channel_record.get("originalName"),
                                self._channel_record.get("_id"))
                        loggers[self._channel_type]["errors"]["logger"].error(err)

                    queue_name = "channels." + self._channel_type + ".items"
                    message = {
                        "importTag": article_import_record.get("importTag"),
                        "channelId":self._channel_record.get("_id"),
                        "url":article_url,
                        "_id":article_import_record.get("_id")
                    }
                    self._consumer.send_message(queue_name, message)
                    loggers[self._channel_type]["info"]["logger"].debug(
                            "Created mongo record for the article %s (%s), channel %s (%s), tag %s",
                            article_import_record.get("_id"), article_url,
                            self._channel_record.get("name") or self._channel_record.get("originalName"),
                            self._channel_record.get("_id"), article_import_record.get("importTag")
                    )

            self.commit_import(isImported)


class ProcessArticleImport(object):
    def __init__(self, message, consumer):
        self._consumer = consumer
        self._attemptNumber = 0
        self._channel_type = consumer.channel_type
        self._client = consumer.http_client
        self._queue_type = consumer.queue_type
        self._message = message
        self._url = None
        self._body = json_decode(message.body.decode('utf_8'))
        self._channel_db = None
        self._channel_blocking_db = None
        self.mobileScreener = consumer.mobileScreener
        self.desktopScreener = consumer.desktopScreener
        try:
            article_task=None
            self._article_record =None
            try:
                self._article_record = articles_queue.find_one({"importTag":self._body["importTag"]})
            except NetworkTimeout:
                self._article_record = articles_queue.find_one({"importTag":self._body["importTag"]})
            if self._article_record:
                server_id = DEFAULT_CHANNELS_SERVER
                db_name = self._article_record.get("channelId")
                try:
                    self._channel_db = server[db_name]
                except couchdb.http.ResourceNotFound:
                    self._channel_db = server.create(db_name)
                    self._channel_db.save({
                        "_id": "_design/articles",
                        "views": {
                            "articles": {
                                "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                            }
                        }
                    })
                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].error(
                            "second attempt to open database %s for article %s(%s), channel %s(%s), tag %s",
                            self._article_record.get("channelName"),
                            self._article_record.get("_id"), self._article_record.get("importUrl"),
                            self._article_record.get("channelId"), self._article_record.get("channelName"),
                            self._body["importTag"])
                    loggers[self._channel_type]["errors"]["logger"].error(err)
                    try:
                        self._channel_db = server[db_name]
                    except couchdb.ResourceNotFound:
                        self._channel_db = server.create(db_name)
                        self._channel_db.save({
                            "_id": "_design/articles",
                            "views": {
                                "articles": {
                                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                                }
                            }
                        })



                self._client_config = CLIENT_CONFIG
                try:
                    self.import_article(self._article_record)
                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].error(
                            "Importing article %s(%s), channel %s(%s), tag %s",
                            self._article_record.get("_id"), self._article_record.get("importUrl"),
                            self._article_record.get("channelId"), self._article_record.get("channelName"),
                            self._body["importTag"])
                    loggers[self._channel_type]["errors"]["logger"].error(err)
                    # self._queue._quantity -= 1
                    self._message.delete()
            else:
                loggers[self._channel_type]["info"]["logger"].debug(
                        "Article already ,tag %s",
                        self._body["importTag"])
                # self._queue._quantity -= 1
                self._message.delete()
                return
        except Exception as err:
            loggers["unknown"]["errors"]["logger"].error("Importing article %s", self._body["importTag"])
            loggers["unknown"]["errors"]["logger"].error(err)
            loggers["unknown"]["errors"]["logger"].error(self._body)
            traceback.print_exc()
            raise

    def mark_success(self, channel_id):
        try:
            channel = channels_queue.find_one({'_id': channel_id})
        except NetworkTimeout:
            channel = channels_queue.find_one({'_id': channel_id})
        if channel:
            channel["lastImport"] = int(time.time() * 1000)
            try:
                channels_queue.replace_one({'_id': channel_id},channel)
            except NetworkTimeout:
                channels_queue.replace_one({'_id': channel_id},channel)

    def commit_import(self):
        try:
            try:
                doc = self._channel_db.get(self._article_record["articleId"])
            except:
                doc = self._channel_db.get(self._article_record["articleId"])
            doc["status"] = "published"
            try:
                _id, _rev = self._channel_db.save(doc)
            except:
                _id, _rev = self._channel_db.save(doc)
            doc["_rev"] = _rev
            oldStatus = "active20"
            status = "active"
            # self._article_record = articles_queue.get(self._article_record["_id"])
            if self._article_record["srcType"] == 'vk':
                oldStatus = 'archived20'
                status = "archived"
                self._article_record["status"] = 'archived20'
                try:
                    articles_queue.replace_one({"_id": self._article_record["_id"]},self._article_record)
                except NetworkTimeout:
                    articles_queue.replace_one({"_id": self._article_record["_id"]},self._article_record)
                self.mark_success(self._article_record["channelId"])
            else:
                self._article_record["status"] = "active20"
                self._article_record["attemptNumber"] = 0
                self._article_record["nextTry"] = int(time.time() * 1000) + 60 * 15 * 1000
                self._article_record["importTag"] = str(uuid4())
                try:
                    articles_queue.replace_one({"_id": self._article_record["_id"]}, self._article_record)
                except NetworkTimeout:
                    articles_queue.replace_one({"_id": self._article_record["_id"]}, self._article_record)

                queue_name = "channels.replication.add"
                message = {"channelId": self._article_record["channelId"], "status": status, "oldStatus": oldStatus,
                           "articleId": self._article_record["articleId"], "_id": self._article_record["_id"],
                           "isMobile": False}
                self._consumer.send_message(queue_name, message)

            loggers[self._article_record["srcType"]]["info"]["logger"].debug("Commit article %s (%s), channel %s (%s)",
                                                                          self._article_record["articleId"],
                                                                          self._article_record["importUrl"],
                                                                          self._article_record["channelName"],
                                                                          self._article_record["channelId"])




        except Exception as err:
            loggers[self._article_record["srcType"]]["errors"]["logger"].error("Commit article %s (%s), channel %s (%s)",
                                                                            self._article_record["articleId"],
                                                                            self._article_record["importUrl"],
                                                                            self._article_record["channelName"],
                                                                            self._article_record["channelId"])
            loggers[self._article_record["srcType"]]["errors"]["logger"].error(err)
            raise
        else:
            if self._message: self._message.delete()

    def save_article(self, attachments=None, article_attrs=None, src=None, extra_keys=None):
        try:
            src = src or self._article_record["importUrl"]
            now = int(time.time() * 1000)
            doc = {
                "_id": self._article_record.get("articleId"),
                "src": src,
                "read": False,
                "title": self._article_record.get("title"),
                "channelId": self._article_record.get("channelId"),
                "serverId": self._article_record.get("serverId"),
                "body": self._article_record.get("body"),
                "channelName": self._article_record.get("channelName"),
                "channelOriginalName": self._article_record.get("channelOriginalName"),
                "categories": self._article_record.get("channelCategories"),
                "attachments": attachments,
                "articleAttrs": article_attrs,
                "status": "importing",
                "pubDate": self._article_record.get("pubDate"),
                "date": now,
                "srcType": self._channel_type,
                "type": "article",
                "feedMode": self._article_record.get("feedMode")
            }
            if extra_keys:
                doc = dict(list(doc.items()) + list(extra_keys.items()))

            try:
                old_doc = self._channel_db.get(doc["_id"])
            except:
                old_doc = self._channel_db.get(doc["_id"])
            if not old_doc:
                try:
                    try:
                        id,rev = self._channel_db.save(doc)
                    except:
                        id,rev = self._channel_db.save(doc)
                    doc["_rev"] = rev
                except Exception as err:
                    raise
            else:
                if old_doc.get("_attachments"):
                    attachments = old_doc.get("_attachments")
                    for name, info in attachments.items():
                        self._channel_db.delete_attachment(old_doc, name)
                doc["_rev"] = old_doc["_rev"]

        except Exception as err:
            doc = None
            loggers[self._article_record["srcType"]]["errors"]["logger"].error("Commit article %s (%s), channel %s (%s)",
                                                                               self._article_record["articleId"],
                                                                               self._article_record["importUrl"],
                                                                               self._article_record["channelName"],
                                                                               self._article_record["channelId"])
            loggers[self._article_record["srcType"]]["errors"]["logger"].error(err)
            raise

        return doc

    @abc.abstractmethod
    def import_article(self, article):
        pass



class ConsumerAdd(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client):
        super().__init__(queue_type)
        self.channel_type = channel_type
        self.queue_type = queue_type
        self.http_client = http_client

    def empty_callback(self):
        channels_db = server["channels"]

    def queue_callback(self, message):
        try:
            body = json_decode(message.body.decode('utf_8'))
            AddChannel(body,self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerUpdate(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client):
        super().__init__(queue_type)
        self._channel_type = channel_type
        self._queue_type = queue_type
        self._channel_type = channel_type
        self._client = http_client
        self._client_config = CLIENT_CONFIG


    def empty_callback(self):
        channels_db = server["channels"]

    def upd_channel(self, updateData):
        channels_db = server["channels"]
        if updateData.get("dbName"):
            dbname=updateData.get("dbName") if updateData.get("dbName")[0]=="c" else updateData.get("_id")
            channels_db = server[dbname]
        try:
            channel_record = channels_queue.find_one({'_id':updateData["_id"]})
        except NetworkTimeout:
            channel_record = channels_queue.find_one({'_id':updateData["_id"]})

        loggers[self._channel_type]["info"]["logger"].debug("Update channel %s", updateData["_id"])
        if "imageUrl" in updateData:
            ext = 'jpg'
            new_src = '#Long-And-Complex-Delimiter#' + updateData["_id"] + \
                      '#Long-And-Complex-Delimiter#channelIcon#Long-And-Complex-Delimiter#'
            response = None
            try:
                response = self._client.fetch(updateData["imageUrl"], **self._client_config)
            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].debug(
                        "second attempt to download icon %s for channel %s",
                        updateData["imageUrl"], updateData["_id"])
                loggers[self._channel_type]["errors"]["logger"].debug(err)
                time.sleep(3)
                try:
                    response = self._client.fetch(updateData["imageUrl"], **self._client_config)
                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].debug(
                            "third attempt to download icon %s for channel %s",
                            updateData["imageUrl"], updateData["_id"])
                    loggers[self._channel_type]["errors"]["logger"].debug(err)
                    time.sleep(3)
                    try:
                        response = self._client.fetch(updateData["imageUrl"], **self._client_config)
                    except Exception as err:
                        loggers[self._channel_type]["errors"]["logger"].debug(
                                "forth attempt to download icon %s for channel %s",
                                updateData["imageUrl"], updateData["_id"])
                        loggers[self._channel_type]["errors"]["logger"].debug(err)
                        time.sleep(3)
                        try:
                            response = self._client.fetch(updateData["imageUrl"], **self._client_config)
                        except Exception as err:
                            pass
            if response :
                try:
                    doc = channels_db.get(updateData["_id"])
                except:
                    doc = channels_db.get(updateData["_id"])
                try:
                    file = io.BytesIO(response.body)
                    result = channels_db.put_attachment(doc, file, filename="channelIcon",
                                                           content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                           response.headers["Content-Type"])
                    doc["imageUrl"] = new_src
                    channel_record["newImageUrl"] = new_src
                    # result = channels_queue.put_attachment(channel_record, file,filename = "channelIcon",
                    #                                       content_type="image/jpeg" if "Content-Type" not in response.headers else response.headers["Content-Type"])
                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].error(
                            "Failed to save icon %s for channel %s",
                            updateData["imageUrl"], updateData["_id"])
                    loggers[self._channel_type]["errors"]["logger"].error(err)
            else:
                loggers[self._channel_type]["errors"]["logger"].error(
                        "Failed to download icon %s for channel %s",
                        updateData["imageUrl"], updateData["_id"])
        else:
            loggers[self._channel_type]["errors"]["logger"].error(
                    "Icon for channel %s is missing", updateData["_id"])

        if "originalName" in updateData or "originalDescription" in updateData:
            try:
                doc = channels_db.get(updateData["_id"])
            except:
                doc = channels_db.get(updateData["_id"])
            if "originalName" in updateData:
                doc["originalName"] = updateData["originalName"]
                channel_record["originalName"] = updateData["originalName"]
            if "originalDescription" in updateData:
                doc["originalDescription"] = updateData["originalDescription"]
                channel_record["originalDescription"] = updateData["originalDescription"]
            try:
                _id, _rev = channels_db.save(doc)
            except:
                _id, _rev = channels_db.save(doc)
            doc["_rev"] = _rev
        if updateData.get("dbName") and updateData.get("dbName","")!="channelsen" and (
                    (self._channel_type == "rss" and channel_record["feedMode"] == "web") or self._channel_type == "lj"):
            copy_channel_to_mobile(updateData["_id"])

        channels_db = None
        try:
            channels_queue.replace_one({"_id":updateData["_id"]},channel_record)
        except NetworkTimeout:
            channels_queue.replace_one({"_id":updateData["_id"]},channel_record)

    def queue_callback(self, message):
        try:
            body = json_decode(message.body.decode('utf_8'))
            self.upd_channel(body)
            message.delete()
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self._channel_type]["errors"]["logger"].error(err)
                loggers[self._channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body.get("importTag",""), body.get("mode"))

            finally:
                message.unlock()


class ConsumerProcessChannelImport(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client,asynch_http_client=None):
        super().__init__(queue_type)
        self.channel_type = channel_type
        self.queue_type = queue_type
        self.channel_type = channel_type
        self.http_client = http_client
        self.asynch_http_client = asynch_http_client


    def empty_callback(self):
        channels_db = server["channels"]

    def queue_callback(self, message):
        try:
            # message = json_decode(message.body)
            ProcessChannelImport(message,self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerProcessArticleImport(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client):
        super().__init__(queue_type)
        self.channel_type = channel_type
        self.queue_type = queue_type
        self.channel_type = channel_type
        self.http_client = http_client
        self.mobileScreener = None
        self.desktopScreener = None

    def empty_callback(self):
        channels_db = server["channels"]

    def queue_callback(self, message):
        try:
            ProcessArticleImport(message, self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerProcessChannelCleanup(BaseConsumer):
    def __init__(self, channel_type, queue_type):
        super().__init__(queue_type)
        # self.channel_type = channel_type
        # self.queue_type = queue_type
        # self.http_client = http_client
        self._quantity = 0
        self._limit = 100
        self._channels_stat = {}

    def cleanup_channel(self, message):
        now = int(time.time() * 1000)
        body= json_decode(message.body.decode('utf_8'))
        if body["channelId"] == 'c_anekdot_ru_export_top':
            print(body["channelId"])

        if body["channelId"] not in self._channels_stat:
            self._channels_stat[body["channelId"]] = 0
        if self._channels_stat[body["channelId"]] < now:
            self._channels_stat[body["channelId"]] = now + 60000
            try:
                if body["channelId"]!="c_putin_stories":
                    channel_db = server[body["channelId"]+ "_50"]
                    limit = DEFAULT_CHANNEL_MAX_ARTICLES_PAID  # if message["limit"] is None else message["limit"]
                    articles_to_clean = None
                    try:
                        articles_to_clean = channel_db.view("articles/articles",reduce=False, include_docs=True, descending=True, skip=limit)
                        try:
                            l=len(articles_to_clean.rows)
                        except Exception as err:
                            try:
                                l = len(articles_to_clean.rows)
                            except Exception as err:
                                raise
                    except couchdb.http.ResourceNotFound:
                        channel_db.save({
                            "_id": "_design/articles",
                            "views": {
                                "articles": {
                                    "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                                }
                            }
                        })
                        articles_to_clean = channel_db.view("articles/articles", reduce=False, include_docs=True, descending=True,
                                                 skip=limit)
                    if articles_to_clean and len(articles_to_clean.rows):
                        docs_to_purge = []
                        for result in articles_to_clean.rows:
                            try:
                                if result["doc"]:
                                    loggers["cleanup"]["info"]["logger"].debug(
                                            'deleted article %s(%s) channel %s(%s)',
                                            result["doc"]["src"], result["doc"]["_id"],
                                            result["doc"]["channelName"], result["doc"]["channelId"])
                                    docs_to_purge.append(result["doc"])
                                if docs_to_purge:
                                    channel_db.purge(docs_to_purge)
                            except Exception as err:
                                loggers["cleanup"]["errors"]["logger"].error(
                                        'deleting article %s(%s) channel %s(%s)',
                                        result["doc"]["src"], result["doc"]["_id"],
                                        result["doc"]["channelName"], result["doc"]["channelId"])
                                loggers["cleanup"]["errors"]["logger"].error(err)
                else:
                    print("putin")
                channel_db = None
                channel_db = server[body["channelId"]]
                limit = DEFAULT_CHANNEL_MAX_ARTICLES  if body["channelId"]!="c_putin_stories" else body["limit"]
                articles_to_clean = None
                try:
                    articles_to_clean = channel_db.view("articles/articles", reduce=False, include_docs=True,
                                                        descending=True, skip=limit)
                    try:
                        l = len(articles_to_clean.rows)
                    except Exception as err:
                        try:
                            l = len(articles_to_clean.rows)
                        except Exception as err:
                            raise
                except couchdb.http.ResourceNotFound:
                    channel_db.save({
                        "_id": "_design/articles",
                        "views": {
                            "articles": {
                                "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                            }
                        }
                    })
                    articles_to_clean = channel_db.view("articles/articles", reduce=False, include_docs=True,
                                                        descending=True,
                                                        skip=limit)
                if articles_to_clean and len(articles_to_clean.rows):
                    docs_to_purge = []
                    for result in articles_to_clean.rows:
                        try:
                            if result["doc"]:
                                try:
                                    article = articles_queue.find_one({"articleId":result["doc"]["_id"]})
                                except NetworkTimeout:
                                    article = articles_queue.find_one({"articleId":result["doc"]["_id"]})
                                if article and (article["status"] == "archived" or article["status"] == "error"):
                                    loggers["cleanup"]["info"]["logger"].debug(
                                            'purged article %s(%s) channel %s(%s)',
                                            result["doc"]["src"], result["doc"]["_id"],
                                            result["doc"]["channelName"], result["doc"]["channelId"])
                                    docs_to_purge.append(result["doc"])
                                    if docs_to_purge:
                                        channel_db.purge(docs_to_purge)
                                elif body["channelId"]=="c_putin_stories":
                                    loggers["cleanup"]["info"]["logger"].debug(
                                    'deleted article %s channel Putin',
                                    result["doc"]["_id"])
                                    # docs_to_purge.append(result["doc"])
                                    channel_db.delete(result["doc"])



                        except Exception as err:
                            loggers["cleanup"]["errors"]["logger"].error(
                                    'deleting article %s(%s) channel %s(%s)',
                                    result["doc"]["src"], result["doc"]["_id"],
                                    result["doc"]["channelName"], result["doc"]["channelId"])
                            loggers["cleanup"]["errors"]["logger"].error(err)
                channel_db = None

                if body["channelId"]!="c_putin_stories":
                    if body["channelId"] + "_m_50" in server:
                        channel_db = server[body["channelId"] + "_m_50"]
                        limit = DEFAULT_CHANNEL_MAX_ARTICLES_PAID  # if message["limit"] is None else message["limit"]
                        articles_to_clean = None
                        try:
                            articles_to_clean = channel_db.view("articles/articles", reduce=False, include_docs=True,
                                                                descending=True, skip=limit)
                            try:
                                l = len(articles_to_clean.rows)
                            except Exception as err:
                                try:
                                    l = len(articles_to_clean.rows)
                                except Exception as err:
                                    raise
                        except couchdb.http.ResourceNotFound:
                            channel_db.save({
                                "_id": "_design/articles",
                                "views": {
                                    "articles": {
                                        "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                                    }
                                }
                            })
                            articles_to_clean = channel_db.view("articles/articles", reduce=False, include_docs=True,
                                                                descending=True,
                                                                skip=limit)

                        if articles_to_clean and len(articles_to_clean.rows):
                            docs_to_purge = []
                            for result in articles_to_clean.rows:
                                try:
                                    if result["doc"]:
                                        loggers["cleanup"]["info"]["logger"].debug(
                                                'deleted article %s(%s) channel %s(%s)',
                                                result["doc"]["src"], result["doc"]["_id"],
                                                result["doc"]["channelName"], result["doc"]["channelId"])
                                        docs_to_purge.append(result["doc"])
                                    if docs_to_purge:
                                        channel_db.purge(docs_to_purge)

                                except Exception as err:
                                    loggers["cleanup"]["errors"]["logger"].error(
                                            'deleting article %s(%s) channel %s(%s)',
                                            result["doc"]["src"], result["doc"]["_id"],
                                            result["doc"]["channelName"], result["doc"]["channelId"])
                                    loggers["cleanup"]["errors"]["logger"].error(err)
                        channel_db = None

                    if body["channelId"] + "_m" in server:
                        channel_db = server[body["channelId"] + "_m"]
                        articles_to_clean = None
                        limit = DEFAULT_CHANNEL_MAX_ARTICLES if body["limit"] is None else body["limit"]
                        try:
                            articles_to_clean = server[body["channelId"] + "_m"].view("articles/articles", reduce=False, include_docs=True,
                                                                descending=True, skip=limit)
                            try:
                                l = len(articles_to_clean.rows)
                            except Exception as err:
                                try:
                                    l = len(articles_to_clean.rows)
                                except Exception as err:
                                    raise
                        except couchdb.http.ResourceNotFound:
                            server[body["channelId"] + "_m"].save({
                                "_id": "_design/articles",
                                "views": {
                                    "articles": {
                                        "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                                    }
                                }
                            })
                            articles_to_clean = server[body["channelId"] + "_m"].view("articles/articles", reduce=False, include_docs=True,
                                                                descending=True,
                                                                skip=limit)
                        if articles_to_clean and len(articles_to_clean.rows):
                            docs_to_purge = []
                            for result in articles_to_clean.rows:
                                try:
                                    if result["doc"]:
                                        try:
                                            article = articles_queue.find_one({"articleId": result["doc"]["_id"]})
                                        except NetworkTimeout:
                                            article = articles_queue.find_one({"articleId": result["doc"]["_id"]})
                                        if article and( article["status"] == "archived" or article["status"] == "error"):
                                            loggers["cleanup"]["info"]["logger"].debug(
                                                    'deleted article %s(%s) channel %s(%s)',
                                                    result["doc"]["src"], result["doc"]["_id"],
                                                    result["doc"]["channelName"], result["doc"]["channelId"])
                                            docs_to_purge.append(result["doc"])
                                        if docs_to_purge:
                                            channel_db.purge(docs_to_purge)
                                            # if need_refresh_size is None:
                                            #     need_refresh_size = True
                                except Exception as err:
                                    loggers["cleanup"]["errors"]["logger"].error(
                                            'deleting article %s(%s) channel %s(%s)',
                                            result["doc"]["src"], result["doc"]["_id"],
                                            result["doc"]["channelName"], result["doc"]["channelId"])
                                    loggers["cleanup"]["errors"]["logger"].error(err)
                        channel_db = None


            except Exception as err:

                loggers["cleanup"]["errors"]["logger"].error(
                        'cleaning channel %s', body["channelId"])
                loggers["cleanup"]["errors"]["logger"].error(body)
                loggers["cleanup"]["errors"]["logger"].error(err)
            finally:
                message.delete()
        else:
            message.delete()


    def empty_callback(self):
        channels_db = server["channels"]

    def queue_callback(self, message):
        try:
            # message = json_decode(message.body)
            self.cleanup_channel(message)
        except Exception as err:
            message.delete()
            message = json_decode(message.body.decode('utf_8'))
            loggers[message["srcType"]]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                  message["_id"],
                                                                  message["importTag"], message["mode"])
            loggers[message["srcType"]]["errors"]["logger"].error(err)
            raise
