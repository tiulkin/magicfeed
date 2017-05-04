# !/usr/bin/env python3
import io
from consumer import *


def get_token():
    return ("b67806afa9ee0e8cf3a746b68c5cfc449e7b00b9e0eedd40f48e8a839c93814c0b53cf861700364b2617c29d3461e");

class ProcessChannelImportVK(ProcessChannelImport):
    def process_import_url(self, http_response):
        channel_to_clean = None
        if http_response.error:
            self._message.unlock()
            return
        response = json_decode(http_response.body)
        if "error" in response:
            self._message.unlock()
            return
        items = response["response"]["wall"]
        isImported = False
        try:
            for index in range(1, len(items)):
                item = items[index]
                # if item["date"] < int(time.time() ) + ARTICLE_IMPORT_DELAY:
                #     continue
                if "id" not in item or "is_pinned" in item and item["is_pinned"]:
                    continue
                if self._channel_record["srcAttrs"]["type"] == "group" or self._channel_record["srcAttrs"]["type"] == "page":
                    article_url = "https://vk.com/wall-" + str(self._channel_record.get("srcAttrs")["id"]) + "_" + str(
                            item["id"])
                else:
                    article_url = "https://vk.com/wall" + str(self._channel_record.get("srcAttrs")["id"]) + "_" + str(
                            item["id"])
                article_md5 = hashlib.md5((self._channel_record.get("_id") + "_" + article_url).encode()).hexdigest()
                try:
                    article_import_record = articles_queue.find_one({"_id":article_md5})
                except NetworkTimeout:
                    article_import_record = articles_queue.find_one({"_id":article_md5})

                attachments = []

                if not article_import_record:
                    try:
                        isImported = True
                        channel_to_clean = self._channel_record["_id"]
                        article_id = str(uuid4())
                        if "attachments" in item:
                            attachments = item["attachments"]

                        body = item["text"]
                        article_import_record = self.add_article_record(article_url, article_id,
                                                               self._channel_record["_id"], self._channel_record.get("serverId"),
                                                               self._channel_record.get("name"),
                                                               self._channel_record.get("originalName"),
                                                               self._channel_record.get("channelCategories"),
                                                               "", 1000 * item["date"],
                                                               body, None, {
                                                                   "postType": item["post_type"],
                                                                   "attachments": attachments
                                                               }, None)
                    except Exception as err:
                        loggers[self._channel_type]["errors"]["logger"].error(
                                "Creation mongo record for the article %s (%s), channel %s (%s)",
                                article_import_record["_id"], article_url,
                                self._channel_record["name"] or self._channel_record["originalName"],
                                self._channel_record["_id"])
                        loggers[self._channel_type]["errors"]["logger"].error(err)

                    queue_name = "channels." + self._channel_type + ".items"
                    message = {"importTag": article_import_record.get("importTag")}
                    self._consumer.send_message(queue_name, message)
                    loggers[self._channel_type]["info"]["logger"].debug(
                            "Created mongo record for the article %s (%s), channel %s (%s), tag %s",
                            article_import_record["_id"], article_url,
                            self._channel_record.get("name") or self._channel_record.get("originalName"),
                            self._channel_record["_id"], article_import_record.get("importTag")
                    )
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error(err)
            self._message.unlock()
        else:
            self.commit_import(isImported)

        if channel_to_clean:
            queue_name = "channels.clean.custom_1"
            message = {"channelId": channel_to_clean, "serverId": self._channel_record.get("serverId"), "limit": None}
            self._consumer.send_message(queue_name, message)
            loggers[self._channel_type]["info"]["logger"].debug(
                    "Sent message to cleanup channel %s (%s)",
                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                    self._channel_record["_id"]
            )
            loggers["cleanup"]["info"]["logger"].debug(
                    "Sent message to clean channel %s (%s)",
                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                    self._channel_record["_id"]
            )


class AddChannelVK(AddChannel):
    def get_url(self):
        try:
            if "url" in self._request_doc["channelData"]:
                href = "https://api.vk.com/method/wall.get?extended=1&domain=" + self._request_doc["channelData"]["url"]
            else:
                return None
            tldextract.extract(href)
            url = urllib.parse.urlparse(href)
            return href

        except Exception as err:
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(
                "Check url %s for request %s",
                self._request_doc["channelData"]["url"], self._request_doc["_id"])
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(err)
            return None

    def check_channel_step2(self, http_response):
        if http_response.error:
            return
        response = json_decode(http_response.body)
        channel_id = "c_" + self.locale_ext + self._request_doc["_id"]
        try:
            self._channel_record = channels_queue.find_one({"_id":channel_id})
        except NetworkTimeout:
            self._channel_record = channels_queue.find_one({"_id":channel_id})
        if self._channel_record and self._channel_record["status"] == "checking":
            try:
                self._channel_doc = self._channels_db.get(channel_id)
            except:
                self._channel_doc = self._channels_db.get(channel_id)

            if self._channel_doc:
                self._channel_record["state"] = "detectType"
                try:
                    channels_queue.replace_one({"_id": channel_id},self._channel_record)
                except NetworkTimeout:
                    channels_queue.replace_one({"_id": channel_id},self._channel_record)
                href = "https://api.vk.com/method/groups.getById?fields=description,name,photo_100&group_ids=" + \
                    self._channel_record["srcAttrs"]["domain"]
                try:
                    new_response = self._client.fetch(href, **self._client_config)
                except httpclient.HTTPError as err:
                # except Exception as err:
                    if self._channel_record["state"] == "detectType":
                        try:
                            href = "https://api.vk.com/method/users.get?fields=description,name,photo_100&user_ids=" + \
                                   self._channel_record.get("srcAttrs")["domain"]
                            loggers[self._channel_type]["info"]["logger"].debug(
                                    "Trying to detect type of channel %s (%s), url %s, user %s",
                                    "c_" + self.locale_ext + self._request_doc["_id"],
                                    self._channel_record.get("name") or self._channel_record.get("originalName"),
                                    href, self._db_name)

                            self._channel_record["state"] = "final"
                            try:
                                channels_queue.replace_one({"_id": channel_id},self._channel_record)
                            except NetworkTimeout:
                                channels_queue.replace_one({"_id": channel_id},self._channel_record)
                            result = self._client.fetch(href, **self._client_config)

                            self.check_channel_step2(result)
                        except:
                            self._channel_record["state"] = "detectType"
                            try:
                                channels_queue.replace_one({"_id": channel_id}, self._channel_record)
                            except NetworkTimeout:
                                channels_queue.replace_one({"_id": channel_id}, self._channel_record)
                    else:
                        loggers[self._channel_type]["errors"]["logger"].error(
                                "Error in response for channel %s (%s), url %s, user %s",
                                "c_" + self.locale_ext + self._request_doc["_id"],
                                self._channel_record.get("name") or self._channel_record.get("originalName"),
                                self._channel_record.get("srcUrl"), self._db_name)
                else:
                    self.add_channel(new_response)

    def add_channel(self, http_response):
        try:
            response = json_decode(http_response.body)
            channel_id = "c_" + self.locale_ext + self._request_doc["_id"]
            if self._channel_record and self._channel_record["status"] == "checking":
                try:
                    self._channel_doc = self._channels_db.get(channel_id)
                except:
                    self._channel_doc = self._channels_db.get(channel_id)
                if self._channel_doc:
                    self._channel_record["lastCheck"] = int(time.time() * 1000)
                    try:
                        self._channel_doc = channels_queue.find_one({"_id": channel_id})
                    except NetworkTimeout:
                        self._channel_doc = channels_queue.find_one({"_id": channel_id})
                    if self._channel_doc:
                        self._channel_record["lastCheck"] = int(time.time() * 1000)
                        update_data = self.get_updateData(response)
                        success = self.commit_channel()
                        if not success:
                            return
                        # time.sleep(1)
                        queue_name = "channels." + self._channel_type + ".process"
                        message = {"importTag": self._channel_record.get("importTag")}
                        self._consumer.send_message(queue_name, message)

                        if update_data:
                            queue_name = "channels." + self._channel_type + ".update"
                            update_data["_id"] = self._channel_record["_id"]
                            update_data["dbName"] = self._channel_record["dbName"]
                            update_data["serverId"] = self._channel_record["creatorServerId"]
                            self._consumer.send_message(queue_name, update_data)
                    else:
                        loggers[self._channel_type]["info"]["logger"].debug("Channel record %s not found in couch",
                                                                            "c_" + self.locale_ext + self._request_doc["_id"])
            else:
                loggers[self._channel_type]["info"]["logger"].debug("Channel record %s not found in mongo",
                                                                    "c_" + self.locale_ext + self._request_doc["_id"])
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Add channel %s ,user %s",
                                                                  "c_" + self.locale_ext + self._request_doc["_id"], self._db_name)
            loggers[self._channel_type]["errors"]["logger"].error(err)

    def check_channel(self,body=None):
        try:
            if self._mode == "add":
                href = self.get_url()
                if href:
                    channel_doc = self.get_channel_couch(href)
                else:
                    self.commit_error()
                    return
                src_attrs = {"domain": self._request_doc["channelData"]["url"], "type": "", "id": "", "isClosed": ""}
                new_mongo_channel = self.add_channel_mongo(channel_doc, src_attrs)
                if not new_mongo_channel:
                    return
                success = self.commit_request()
                if not success:
                    return
            else:
                if self._channel_record.get("state") == "checkPage":
                    href = self.get_url()
                    # href = self._channel_record.srcUrl
                    try:
                        response = self._client.fetch(href)

                        self.check_channel_step2(response)
                    except:
                        pass
                elif self._channel_record.get("state") == "detectType":
                    href = "https://api.vk.com/method/groups.getById?fields=description,name,photo_100&group_ids=" + \
                           self._channel_record.get("srcAttrs")["domain"]
                    try:
                        response = self._client.fetch(href)

                        self.add_channel(response)
                    except Exception as err:
                        print(err)
                else:  # self._channel_record.state == "final":
                    href = "https://api.vk.com/method/users.get?fields=description,name,photo_100&user_ids=" + \
                           self._channel_record.get("srcAttrs")["domain"]
                    try:
                        response = self._client.fetch(href)

                        self.check_channel_step2(response)
                    except Exception as err:
                        print(err)

            response = self._client.fetch(href)

            self.check_channel_step2(response)


        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Check channel")
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise

    def get_updateData(self, response, data=None):
        try:
            updateData = {
                "_id": self._channel_record["_id"]
            }
            if self._channel_record.get("state") == "detectType":
                self._channel_record["srcAttrs"] = {
                    "domain": self._channel_record.get("srcAttrs")["domain"],
                    "type": "group",
                    "id": response["response"][0]["gid"],
                    "is_closed": response["response"][0]["is_closed"]
                }
                self._channel_record["originalName"] = response["response"][0]["name"]
                self._channel_record["originalDescription"] = response["response"][0]["description"]
                self._channel_doc["originalName"] = response["response"][0]["name"]
                self._channel_doc["originalDescription"] = response["response"][0]["description"]
                updateData["originalDescription"] = self._channel_record.get("originalDescription")
            else:
                self._channel_record.srcAttrs = {
                    "domain": self._channel_record.get["srcAttrs"]["domain"],
                    "type": "user",
                    "id": response["response"][0]["uid"],
                    "is_closed": response["response"][0]["is_closed"]
                }
                self._channel_doc["originalName"] = response["response"][0]["first_name"] + ' ' + \
                                                    response["response"][0]["last_name"]

                self._channel_record["originalName"] = response["response"][0]["first_name"] + ' ' + \
                                                    response["response"][0]["last_name"]

            if "photo_100" in response["response"][0]:
                self._channel_record["imageUrl"] = response["response"][0]["photo_100"]
            elif "photo_big" in response["response"][0]:
                self._channel_record["imageUrl"] = response["response"][0]["photo_big"]
            elif "photo" in response["response"][0]:
                self._channel_record["imageUrl"] = response["response"][0]["photo"]
            if self._channel_record["imageUrl"]:
                updateData["imageUrl"] = self._channel_record["imageUrl"]

            updateData["originalName"] = self._channel_record.get("originalName")
            prefix=""
            if self._channel_record.get("srcAttrs")["type"] == "group" or self._channel_record.get("srcAttrs")["type"] == "page":
                prefix = "-"
            srcUrl = "https://api.vk.com/method/wall.get?extended=1&owner_id=" + prefix + str(
                    self._channel_record["srcAttrs"]["id"])
            updateData["srcUrl"] = srcUrl
            self._channel_doc["srcUrl"] = srcUrl
            self._channel_record["srcUrl"] = srcUrl

            try:
                channels_queue.replace_one({"_id": self._channel_record["_id"]}, self._channel_record)
            except NetworkTimeout:
                channels_queue.replace_one({"_id": self._channel_record["_id"]}, self._channel_record)
            return updateData


        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error("Update data for article %s",
                                                                  self._channel_record["_id"])
            loggers[self._channel_type]["errors"]["logger"].error(err)
            raise


class ProcessArticleImportVK(ProcessArticleImport):
    def get_file(self, url, doc):
        try:
            response = self._client.fetch(url, **self._client_config)

        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].debug("second attempt to download file %s", url)
            loggers[self._channel_type]["errors"]["logger"].debug(err)
            time.sleep(3)
            try:
                response = self._client.fetch(url, **self._client_config)

            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].debug("third attempt to download file %s", url)
                loggers[self._channel_type]["errors"]["logger"].debug(err)
                time.sleep(30)
                try:
                    response = self._client.fetch(url, **self._client_config)

                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].debug("forth attempt to download file %s", url)
                    loggers[self._channel_type]["errors"]["logger"].debug(err)
                    time.sleep(60)
                    try:
                        response = self._client.fetch(url, **self._client_config)

                    except Exception as err:
                        loggers[self._channel_type]["errors"]["logger"].debug("failed to download file %s", url)
                        loggers[self._channel_type]["errors"]["logger"].debug(err)
                        response = None
        return (response)

    def import_photo(self, doc, attachment):
        result = {}
        if "photo" in attachment:
            photo = attachment["photo"]
        else:
            photo = attachment
        filename = str(uuid4())
        if "access_key" in photo:
            access_key = ""  # "?access_key=" + photo["access_key"]
        else:
            access_key = ""
        if "src_big" in photo:
            imageUrl = url_fix(photo["src_big"]) + access_key
            response = self.get_file(imageUrl, doc)
            if response and response.body:
                try:
                    doc = self._channel_db.get(doc["_id"])
                except:
                    doc = self._channel_db.get(doc["_id"])
                try:
                    file = io.BytesIO(response.body)
                    try:
                        self._channel_db.put_attachment(doc, file, filename=filename,
                                                                   content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                                   response.headers["Content-Type"])
                    except:
                        self._channel_db.put_attachment(doc, file, filename=filename,
                                                        content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                        response.headers["Content-Type"])
                    result = {
                        "type": "image",
                        "fileName": filename,
                        "text": photo["text"]
                    }
                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].error("can''t save the photo %s for %s (%s)",
                                                                          imageUrl, doc["title"], doc["src"])
                    loggers[self._channel_type]["errors"]["logger"].error(err)
        return (result)

    def import_link(self, doc, attachment):
        filename = str(uuid4())
        result = {
            "type": "link",
            "title": attachment["link"]["title"],
            "url": attachment["link"]["url"],
            "text": ""
        }
        if "description" in attachment["link"]:
            result["text"] = attachment["link"]["description"]

        if "image_src" in attachment["link"]:
            imageUrl = url_fix(attachment["link"]["image_src"])
            response = self.get_file(imageUrl, doc)
            if response and response.body:
                try:
                    doc = self._channel_db.get(doc["_id"])
                except:
                    doc = self._channel_db.get(doc["_id"])
            try:
                file = io.BytesIO(response.body)
                try:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                         content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                         response.headers["Content-Type"])
                except:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                    content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                    response.headers["Content-Type"])
                result = {
                    "fileName": filename
                }

            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].error("can''t save the link %s for %s (%s)",
                                                                      imageUrl, doc["title"], doc["src"])
                loggers[self._channel_type]["errors"]["logger"].error(err)
                result = {}
            else:
                result = {}
        else:
            result = {}
        return (result)

    def import_doc(self, doc, attachment):
        filename = str(uuid4())
        type = "unknown"
        result = {}
        if "access_key" in attachment["doc"]:
            access_key = ""  # "&access_key=" + attachment["doc"]["access_key"]
        else:
            access_key = ""

        if "ext" in attachment["doc"]:
            if attachment["doc"]["ext"] == "gif":
                type = "gif"
        if type == "gif" and "url" in attachment["doc"]:
            imageUrl = url_fix(attachment["doc"]["url"]) + access_key

            response = self.get_file(imageUrl, doc)
            if response and response.body:
                try:
                    doc = self._channel_db.get(doc["_id"])
                except:
                    doc = self._channel_db.get(doc["_id"])
            try:
                file = io.BytesIO(response.body)
                try:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                         content_type="image/gif" if "Content-Type" not in response.headers else
                                                         response.headers["Content-Type"])
                except:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                    content_type="image/gif" if "Content-Type" not in response.headers else
                                                    response.headers["Content-Type"])

                result = {
                    "type": "image",
                    "fileName": filename,
                    "text": attachment["doc"]["title"]
                }
            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].error("can''t save the doc %s for %s (%s)",
                                                                      imageUrl, doc["title"], doc["src"])
                loggers[self._channel_type]["errors"]["logger"].error(err)
        else:
            loggers[self._channel_type]["errors"]["logger"].error("unknown doc type %s for %s (%s)",
                                                                  type, doc["title"], doc["src"])

        return (result)

    def import_video(self, doc, attachment):
        filename = str(uuid4())
        type = "unknown"
        # if "access_key" in attachment["video"]:
        # access_key = "&access_key=" + attachment["video"]["access_key"]
        # else:
        access_key = ""
        result = {
            "type": "video",
            "title": attachment["video"]["title"],
            "text": ""
        }
        if "description" in attachment["video"]:
            result["text"] = attachment["video"]["description"]

        if "date" in attachment["video"]:
            result["date"] = attachment["video"]["date"]

        if "duration" in attachment["video"]:
            result["duration"] = attachment["video"]["duration"]

        if "image_big" in attachment["video"]:
            imageUrl = url_fix(attachment["video"]["image_big"]) + access_key
        elif "image_xbig" in attachment["video"]:
            imageUrl = url_fix(attachment["video"]["image_xbig"]) + access_key
        else:
            imageUrl = url_fix(attachment["video"]["image"]) + access_key

        if imageUrl:
            response = self.get_file(imageUrl, doc)
            if response and response.body:
                try:
                    doc = self._channel_db.get(doc["_id"])
                except:
                    doc = self._channel_db.get(doc["_id"])
            try:
                file = io.BytesIO(response.body)
                try:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                         content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                         response.headers["Content-Type"])
                except:
                    self._channel_db.put_attachment(doc, file, filename=filename,
                                                    content_type="image/jpeg" if "Content-Type" not in response.headers else
                                                    response.headers["Content-Type"])
            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].error(
                    "can''t save the video screenshot %s for %s (%s)",
                    imageUrl, doc["title"], doc["src"])
                loggers[self._channel_type]["errors"]["logger"].error(err)
            result["fileName"] = filename

        else:
            result = {}
        return (result)

    def import_page(self, doc, attachment):
        filename = str(uuid4())
        result = {
            "fileName": filename,
            "type": "page",
            "title": attachment["page"]["title"],
            "text": "",
            "view_url": ""

        }
        if "description" in attachment["page"]:
            result["text"] = attachment["page"]["description"]

        if "view_url" in attachment["page"]:
            result["view_url"] = attachment["page"]["view_url"]

        result["fileName"] = filename
        now = int(time.time() * 1000)
        try:
            loggers[self._channel_type]["info"]["logger"].debug(
                    "create page task %s for article %s, channel %s",
                    str(uuid4()), doc["_id"], doc["channelId"])
            page_task_record = {
                "_id": str(uuid4()),
                "channelId": doc["channelId"],
                "serverId": doc["serverId"],
                "taskType": "vkPage",
                "attemptNumber": 0,
                "taskDetails": {
                    "fileName": filename,
                    "pid": str(attachment["page"]["pid"]),
                    "gid": str(attachment["page"]["gid"])
                },
                "articleId": doc["_id"],
                "date": now,
                "status": "new",
                "srcType": self._channel_type,
                "errorsCount": 0,
                "importTag": "",
                "lastTry": time.time() * 1000,
                "nextTry": time.time() * 1000
            }
            try:
                vk_tasks.replace_one({"_id":page_task_record["_id"]},page_task_record,True)
            except NetworkTimeout:
                vk_tasks.replace_one({"_id":page_task_record["_id"]},page_task_record,True)
            # yield gen.Task(page_task_record.update)
        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error(
                    "create page task %s for article %s, channel %s", doc["_id"], doc["_id"], doc["channelId"])
            loggers[self._channel_type]["errors"]["logger"].error(err)

        return (result)

    def import_album(self, doc, attachment):
        filename = str(uuid4())
        type = "unknown"
        result = {
            "parent": {
                "type": "album",
                "title": attachment["album"]["title"],
                "text": ""
            },
            "children": []
        }
        if "description" in attachment["album"]:
            result["parent"]["text"] = attachment["album"]["description"]

        if "size" in attachment["album"]:
            result["parent"]["size"] = attachment["album"]["size"]

        result["parent"]["fileName"] = filename
        now = int(time.time() * 1000)
        url = "https://api.vk.com/method/photos.get?owner_id=" + str(
                attachment["album"]["owner_id"]) + "&album_id=" + \
              attachment["album"]["aid"]
        # http_response = yield self.get_album(url, doc)

        http_response = None
        try:
            http_response = self._client.fetch(url, **self._client_config)

        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].debug(
                "second attempt to download album %s from %s for article %s, channel %s",
                attachment["album"]["title"], url, doc["_id"], doc["channelId"])
            loggers[self._channel_type]["errors"]["logger"].debug(err)
            time.sleep(3)
            try:
                http_response = self._client.fetch(url, **self._client_config)

            except Exception as err:
                loggers[self._channel_type]["errors"]["logger"].debug(
                        "third attempt to download album %s from %s for article %s, channel %s",
                        attachment["album"]["title"], url, doc["_id"], doc["channelId"])
                loggers[self._channel_type]["errors"]["logger"].debug(err)
                time.sleep(3)
                try:
                    http_response = self._client.fetch(url, **self._client_config)

                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].debug(
                            "fourth attempt to download album %s from %s for article %s, channel %s",
                            attachment["album"]["title"], url, doc["_id"], doc["channelId"])
                    loggers[self._channel_type]["errors"]["logger"].debug(err)
                    time.sleep(3)

        if http_response and http_response.body:
            response = json_decode(http_response.body)
            if "response" in response:
                photos = response["response"]
                counter = 0
                for index in range(len(photos)):
                    counter += 1
                    if counter < 80:
                        attachment = photos[index]
                        try:
                            attachment_for_couch = self.import_photo(doc=doc, attachment=attachment)
                            if attachment_for_couch:
                                result["children"].append(attachment_for_couch)
                        except Exception as err:
                            loggers[self._channel_type]["errors"]["logger"].error(
                                    "Failed to download photo %s for album %s for article %s, channel %s",
                                    photos[index], attachment["album"]["title"], doc["_id"], doc["channelId"])
                            loggers[self._channel_type]["errors"]["logger"].error(err)

        else:
            loggers[self._channel_type]["errors"]["logger"].error(
                    "Failed to download album %s from %s for article %s, channel %s",
                    attachment["album"]["title"], url, doc["_id"], doc["channelId"])
            # loggers[self._channel_type]["errors"]["logger"].debug(http_response.error)

        return (result)

    def import_article(self, article):
        now = int(time.time() * 1000)
        # article.update(safe=True)
        try:
            loggers[self._channel_type]["info"]["logger"].debug("import article %s(%s) ,channel %s",
                                                                article["_id"], article["importUrl"], article["channelName"])
            # yield gen.Task(article.update)
            extra_keys = {"postType": article.get("articleAttrs").get("postType")}
            doc = self.save_article(None, None, None, extra_keys)
            if "attachments" in article.get("articleAttrs"):
                attachments = article.get("articleAttrs")["attachments"]
            else:
                attachments = []
            attachments_for_couch = {}
            audio = []
            counter = 0
            for index in range(len(attachments)):
                try:
                    if index < 80:
                        attachment = attachments[index]
                        if attachment["type"] in ["photo", "doc", "link", "video", "album", "page"]:
                            method = getattr(ProcessArticleImportVK, "import_" + attachment["type"])
                            if method:
                                attachment_for_couch = method(self=self, doc=doc, attachment=attachment)
                                if attachment_for_couch:
                                    if "children" not in attachment_for_couch:
                                        attachments_for_couch[attachment_for_couch["fileName"]] = attachment_for_couch
                                    else:
                                        if "parent" in attachments_for_couch:
                                            attachments_for_couch[attachment_for_couch["parent"]["filename"]] = \
                                                attachment_for_couch["parent"]
                                            attachments_for_couch[attachment_for_couch["parent"]["filename"]][
                                                "children"] = \
                                                attachment_for_couch["children"]
                        elif attachment["type"] == "audio":
                            info = {}
                            audioData = {}
                            if "artist" in attachment:
                                audioData["artist"] = attachment["artist"]
                            if "title" in attachment:
                                audioData["title"] = attachment["title"]
                            if "performer" in attachment:
                                audioData["performer"] = attachment["performer"]
                            audio.append(
                                    audioData
                            )
                        else:
                            loggers[self._channel_type]["info"]["logger"].info(
                                "unknown attachment %s in article %s(%s) ,channel %s",
                                attachment["type"], article["_id"], article["importUrl"], article["channelName"])
                        if len(audio) > 0:
                            attachments_for_couch["audio"] = audio

                except Exception as err:
                    loggers[self._channel_type]["errors"]["logger"].error(
                            "Failed to get attachment %s article %s (%s), channel %s",
                            attachments[index], article["_id"], article["importUrl"], article["channelName"])
                    loggers[self._channel_type]["errors"]["logger"].error(err)

            try:
                doc = self._channel_db.get(doc["_id"])
            except:
                doc = self._channel_db.get(doc["_id"])
            doc["status"] = "published"
            doc["attachments"] = attachments_for_couch
            try:
                _id,_rev = self._channel_db.save(doc)
            except:
                _id,_rev = self._channel_db.save(doc)
            doc["rev"] = _rev
            article["status"] = 'archived20'
            article["attemptNumber"] = 0
            article["nextTry"] = int(time.time() * 1000) + 60 * 5 * 1000
            try:
                articles_queue.replace_one({"_id":article["_id"]},article)
            except NetworkTimeout:
                articles_queue.replace_one({"_id":article["_id"]},article)
            queue_name = "channels.replication.add"
            message = {"channelId": article["channelId"], "status": "archived", "oldStatus": "archived20",
                       "articleId": article["articleId"], "_id": article["_id"], "isMobile": False}
            self._consumer.send_message(queue_name, message)
            self.mark_success(article["channelId"])
            # self._queue._quantity -= 1
            self._message.delete()
            loggers[self._channel_type]["info"]["logger"].debug("Commit import article %s(%s) ,channel %s",
                                                                article["_id"], article["importUrl"], article["channelName"])

        except Exception as err:
            loggers[self._channel_type]["errors"]["logger"].error(
                    "Failed to import article %s (%s), channel %s",
                    article["_id"], article["importUrl"], article["channelName"])
            loggers[self._channel_type]["errors"]["logger"].error(err)
            self._message.unlock()


class ConsumerGetPageVK(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client):
        super().__init__(queue_type)
        self.queue_type = queue_type
        self.channel_type = channel_type
        self.http_client = http_client
        self.client_config = CLIENT_CONFIG

    def get_captcha(self, sid, captcha, url, doc):
        response = yield gen.Task(self._client.fetch, url + "&captcha_sid=" + sid + "&captcha_key=" + captcha,
                                  **self._client_config)
        print(response.body)

    def import_page(self, message):
        now = int(time.time() * 1000)
        body = json_decode(message.body.decode('utf_8'))

        import_tag = body["importTag"]

        page_record = None
        try:
            page_record = vk_tasks.find_one({"importTag":import_tag})
        except NetworkTimeout:
            page_record = vk_tasks.find_one({"importTag":import_tag})
        if page_record:
            try:
                channel_db = server[page_record.get("channelId")]
            except:
                channel_db = server[page_record.get("channelId")]
            self.access_token = get_token()
            self.articleId = page_record["articleId"]
            url = "https://api.vk.com/method/pages.get?need_html=1&pid=" + body["pid"] + "&gid=" + body[
                "gid"] + "&access_token=" + self.access_token
            loggers[self.channel_type]["info"]["logger"].debug("Import page %s, article %s, tag %s",
                                                                body["pid"], page_record.get("articleId"),
                                                                body["importTag"])

            http_response = self.get_page(url, body)
            response = json_decode(http_response.body)
            if "response" in response:
                item_soup = BeautifulSoup(response["response"]["html"])
                images = item_soup.find_all('img')
                images_list = list()
                for image in images:
                    real_src = image.attrs['src']
                    filename = str(uuid4())
                    ext = 'jpg'
                    new_src = '#Long-And-Complex-Delimiter#' + page_record.get("articleId") + '#Long-And-Complex-Delimiter#' + filename + '#Long-And-Complex-Delimiter#'
                    image.attrs['src'] = new_src
                    images_list.append({"filename": filename, "src": real_src})
                html = str(item_soup)
                try:
                    article_doc = channel_db.get(page_record.get("articleId"))
                except:
                    article_doc = channel_db.get(page_record.get("articleId"))
                if not article_doc:
                    page_record["status"] = "missingArticle"
                    try:
                        vk_tasks.replace_one({"_id":page_record["_id"]}, page_record)
                    except NetworkTimeout:
                        vk_tasks.replace_one({"_id":page_record["_id"]}, page_record)
                    loggers[self.channel_type]["errors"]["logger"].info(
                        "Article %s for page %s is missing in couchDB, tag %s",
                        page_record["articleId"], body["pid"],
                        body["importTag"])
                    return True
                else:
                    if page_record["taskDetails"]["fileName"] in article_doc["attachments"]:
                        article_doc["attachments"][page_record["taskDetails"]["fileName"]]["html"] = html
                        try:
                            result = channel_db.save(article_doc)
                        except:
                            result = channel_db.save(article_doc)
                        loggers["vk"]["errors"]["logger"].error(
                            "can''t save the attachment %s(%s) for article %s",
                            body["pid"], url, article_doc["_id"])

                        loggers["vk"]["info"]["logger"].info("Parsing page %s(%s) for article %s",
                                                             body["pid"], url, article_doc["_id"])

                        try:
                            article_doc = channel_db.get(page_record["articleId"])
                        except:
                            article_doc = channel_db.get(page_record["articleId"])


                        for image in images_list:
                            imageUrl = url_fix(image["src"])
                            image_filename = image["filename"]
                            image_response = self.get_page(imageUrl, article_doc)

                            if image_response and image_response.body:
                                attachment = {
                                    "data": image_response.body,
                                    "mimetype": "image/jpeg" if "Content-Type" not in image_response.headers else
                                    image_response.headers["Content-Type"],
                                    "name": image_filename
                                }
                                file = io.BytesIO(image_response.body)
                                try:
                                    result = channel_db.put_attachment(article_doc, file, filename=image_filename,
                                                                       content_type="image/jpeg" if "Content-Type" not in image_response.headers else
                                                                       image_response.headers["Content-Type"])

                                except Exception as err:
                                    loggers["vk"]["info"]["logger"].error(
                                        "Cant save attachment for page %s(%s) for article %s",
                                        body["pid"], url,article_doc["_id"])
                                    loggers["vk"]["errors"]["logger"].error(err)
                                    return False

                            else:
                                loggers["vk"]["info"]["logger"].error(
                                        "Cant save attachment %s for page %s(%s) for article %s",
                                        imageUrl, body["pid"], url, article_doc["_id"])
                                return False



                        page_record["status"] = "done"
                        try:
                            vk_tasks.replace_one({"_id": page_record["_id"]},page_record)
                        except NetworkTimeout:
                            vk_tasks.replace_one({"_id": page_record["_id"]},page_record)


                        loggers[self.channel_type]["info"]["logger"].debug("Imported page %s, article %s, tag %s",
                                                                            body["pid"], page_record["articleId"],
                                                                            body["importTag"])
                        return True
        return False

    def get_page(self, url, doc):
        try:
            response = self.http_client.fetch(url, **self.client_config)
            #
        except Exception as err:
            loggers[self.channel_type]["errors"]["logger"].debug("second attempt to download file %s", url)
            loggers[self.channel_type]["errors"]["logger"].debug(err)
            time.sleep(3)
            try:
                response = self.http_client.fetch(url, **self.client_config)

            except Exception as err:
                loggers[self.channel_type]["errors"]["logger"].debug("third attempt to download file %s", url)
                loggers[self.channel_type]["errors"]["logger"].debug(err)
                time.sleep(30)
                try:
                    response = self.http_client.fetch(url, **self.client_config)

                except Exception as err:
                    loggers[self.channel_type]["errors"]["logger"].debug("forth attempt to download file %s", url)
                    loggers[self.channel_type]["errors"]["logger"].debug(err)
                    time.sleep(60)
                    try:
                        response = self.http_client.fetch(url, **self.client_config)

                    except Exception as err:
                        loggers[self.channel_type]["errors"]["logger"].debug("failed to download file %s", url)
                        loggers[self.channel_type]["errors"]["logger"].debug(err)
                        response = None
        return (response)


    def empty_callback(self):
        channels_db = server["channels"]


    def queue_callback(self, message):
        try:
            self.import_page(message)
            message.delete()
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                               body["_id"],
                                                               body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerAddVK(ConsumerAdd):
    def queue_callback(self, message):
        try:
            body = json_decode(message.body.decode('utf_8'))
            AddChannelVK(body, self)
        except Exception as err:
            message.unlock()
            loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                               body["_id"],
                                                               body["importTag"], body["mode"])
            loggers[self.channel_type]["errors"]["logger"].error(err)

        else:
            message.delete()



class ConsumerProcessChannelImportVK(ConsumerProcessChannelImport):

    def queue_callback(self, message):
        try:
            ProcessChannelImportVK(message, self)
        except Exception as err:
            try:
                loggers["vk"]["errors"]["logger"].error(err)
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()

        # else:
            # if message and message.body:
                # message.delete()


class ConsumerProcessArticleImportVK(ConsumerProcessArticleImport):

    def queue_callback(self, message):
        try:
            ProcessArticleImportVK(message, self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers["vk"]["errors"]["logger"].error("Process article %s with tag %s",
                                                                   body["_id"],
                                                                   body["importTag"])
                loggers["vk"]["errors"]["logger"].error(err)
            finally:
                message.unlock()


