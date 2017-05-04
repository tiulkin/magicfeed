__author__ = 'oleg'
#!/usr/bin/env python3
import sys
import datetime
import base64
import couchdb
import couch
import tornado.web
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.escape
import time
from tornado.ioloop import IOLoop
from uuid import uuid4
from tornado import gen
from tornado.web import RequestHandler, Application, url
from tornado.escape import json_decode, json_encode, url_escape
from tornado_cors import CorsMixin
from custom_couch import CustomAsincCouch
from constants import *
from loggers import loggers
from couchdb import http
from pymongo import *
from pymongo.errors import NetworkTimeout

INITIAL_CHANNELS = ('c_news_mail_ru_main',)
HEADERS = {}

connection = None
channel = None
target_server_id = "azure_internal"
target_server_url = "http://" + COUCH_USERS_CREDS["creds"] + "@" + COUCH_SERVERS[target_server_id]["host"]
internal_server_url = "http://" + COUCH_INTERNAL_CREDS["creds"] + "@" + COUCH_SERVERS["azure_internal"]["host"]

# server_ip = COUCH_SERVERS[DEFAULT_REQUESTS_SERVER]["host"]
db = CustomAsincCouch('requests', internal_server_url)


# source_server_url = "http://" + COUCH_CHANNELS_CREDS["creds"] + "@" + COUCH_SERVERS[source_server_id]["host"]

# server = couchdb.client.Server(target_server_url)
# channels_db = server[COUCH_CHANNELS_LIST_PRIVATE["database"]]
# requests_db = server[COUCH_REQUESTS_PRIVATE["database"]]

internal_server = couchdb.client.Server(internal_server_url, session=http.Session(timeout=60))
internal_server.init_time = time.time()
client = MongoClient(MONGO_SERVER_IP,socketKeepAlive=True, socketTimeoutMS=30000)
mfdb = client.mf
channels_queue = mfdb.ChannelsQueue


def require_basic_auth(handler_class):
    def wrap_execute(handler_execute):
        def require_basic_auth(handler, kwargs):
            if (handler.request.method == 'OPTIONS'):
                return True
            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                handler.set_status(401)
                handler.set_header('WWW-Authenticate', 'Basic realm=Restricted')
                handler._transforms = []
                handler.finish()
                return False
            auth_decoded = base64.b64decode(bytes(auth_header[6:], "utf-8")).decode("utf-8")
            kwargs['basicauth_user'], kwargs['basicauth_pass'] = auth_decoded.split(':', 2)
            return True

        def _execute(self, transforms, *args, **kwargs):
            if not require_basic_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)

        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class


@require_basic_auth
class AddChannelHandler(CorsMixin, RequestHandler):
    CORS_ORIGIN = 'http://127.0.0.1'
    CORS_HEADERS = 'authorization, content-type'
    CORS_METHODS = 'POST'
    CORS_CREDENTIALS = True
    CORS_MAX_AGE = 21600



    def initialize(self):
        print(0)
        self.requests_db = CustomAsincCouch(COUCH_REQUESTS_PRIVATE["database"], COUCH_REQUESTS_PRIVATE["host_auth"])
        self.complains_db = CustomAsincCouch("complains", COUCH_REQUESTS_PRIVATE["host_auth"])

    @gen.coroutine
    def get(self, basicauth_user, basicauth_pass):
        self.write('You are not a registered user')
        self.set_header('WWW-Authenticate', 'Basic realm=Restricted')
        self._transforms = []
        # self.finish()
        # return False

    @gen.coroutine
    def post(self, **kwargs):
        self.add_header('Access-Control-Allow-Credentials', 'true')
        response = {}

        if kwargs['basicauth_user'] != 'megalenta' or kwargs['basicauth_pass'] != 'm#g@lEnt!':
            self.write('You are not registered user')
            self.set_header('WWW-Authenticate', 'Basic realm=Restricted')
            self.finish()
        else:
            try:
                # application=0
                data = tornado.escape.json_decode(self.request.body)
            except:
                self.write('The data is wrong')
                self.set_header('WWW-Authenticate', 'Basic realm=Restricted')
            else:
                db = None
                response = None
                sentData = []
                complains = []
                try:
                    for i in range(0,len(data),1):
                        channel_to_import = ""
                        if data[i] and "channelData" in data[i] and data[i]["channelData"] and \
                                        "sourceType" in data[i]["channelData"] and data[i]["channelData"]["sourceType"] and \
                                        "url" in data[i]["channelData"] and data[i]["channelData"]["url"]:
                        # Посмотрим такой же канал у других
                            srcType = data[i]["channelData"]["sourceType"]
                            user_server_id = DEFAULT_USERS_SERVER if "serverId" not in data[i] else data[i]["serverId"]
                            try:
                                channel_to_import = channels_queue.find_one({
                                    'status': 'active',
                                    "channelData.sourceType": data[i]["channelData"][
                                        "sourceType"],
                                    "channelData.url": data[i]["channelData"]["url"]
                                })
                            except NetworkTimeout:
                                channel_to_import = channels_queue.find_one({
                                    'status': 'active',
                                    "channelData.sourceType": data[i]["channelData"][
                                        "sourceType"],
                                    "channelData.url": data[i]["channelData"]["url"]
                                })


                            if channel_to_import:
                                    sentData.append({"status":"existed", "_id": data[i]["_id"],"name": channel_to_import.get("name"), "channelId":channel_to_import.get("_id")})
                            else:
                                try:
                                    result = yield self.requests_db.get_doc(data[i]["_id"])
                                    if(result["status"]=="error"):
                                        sentData.append({"status": "error", "_id": data[i]["_id"], "name": data[i]["name"] if
                                        "name" in data[i] else "" , "message": result["description"]})
                                except couch.NotFound:
                                    result = yield self.requests_db.save_doc(data[i])
                                    sentData.append({"status": "new", "_id": data[i]["_id"], "name": data[i]["name"] if
                                    "name" in data[i] else "" })

                            print("request ", i, " sent", data)
                        else:
                            if data[i] and "operation" in data[i] and data[i]["operation"]=="complain":
                                print("complain ", i, " sent", data)
                                try:
                                    result = yield self.complains_db.save_doc(data[i])
                                except couch.Conflict:
                                    print("complain ", data, " already registered")
                                sentData.append({"status": "registered", "_id": data[i]["_id"], "name": data[i]["name"] if
                                "name" in data[i] else ""})
                    response = {"status":"sent","requests": sentData}
                    print("response: ", response)

                except Exception as err:
                    loggers["website"]["errors"]["logger"].error(err)
                    loggers["website"]["errors"]["logger"].error(
                        'can''t add site for %s', self.request.body)


                    response = {"status": "error"}
                self.set_header("Content-Type", "application/json; charset=utf8'")
            finally:
                self.write(response)
                self.finish()


# //CorsMixin,
@require_basic_auth
class MainHandler(RequestHandler):
    CORS_ORIGIN = '*'
    CORS_HEADERS = 'authorization, content-type'
    CORS_METHODS = 'POST'
    CORS_CREDENTIALS = True
    CORS_MAX_AGE = 21600


    def get(self, basicauth_user, basicauth_pass):
        self.write('Hi there, {0}!  Your password is {1}.' \
                   .format(basicauth_user, basicauth_pass))

    @gen.coroutine
    def post(self, **kwargs):
        basicauth_user = kwargs['basicauth_user']
        basicauth_pass = kwargs['basicauth_pass']
        self.write('Hi there, {0}!  Your password is {1}.' \
                   .format(basicauth_user, basicauth_pass))


if __name__ == "__main__":
    application = Application([
        url(r'/', MainHandler),
        url(r'/addChannel', AddChannelHandler)
    ])
    application.listen(8887)
    #4474
    tornado.ioloop.IOLoop.instance().start()