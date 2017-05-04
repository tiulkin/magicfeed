# !/usr/bin/env python3
import pdfkit
import io
import os
from consumer import *
from webkit2pdf import createScreenshot
import multiprocessing

def getScreenShot(url,file,mode,logger,cookies,timeout):

    createScreenshot(url_fix(url),file,"desktop" if mode not in("mobile") else "mobile",logger,cookies,timeout)
    return

class ProcessChannelImportRSS(ProcessChannelImport):
    def get_article_attrs(self, id, item):
        if self._channel_record.get("feedMode") == "rss":
            item_soup = BeautifulSoup(item["full_text"])
            images = item_soup.find_all('img')
            images_list = list()
            for image in images:
                try:
                    real_src = image.attrs['src']
                    filename = str(uuid4())
                    ext = 'jpg'
                    # image.attrs['src']
                    new_src = '#Long-And-Complex-Delimiter#' + id + '#Long-And-Complex-Delimiter#' + filename + '#Long-And-Complex-Delimiter#'
                    image.attrs['src'] = new_src
                    if real_src.find("http") == -1:
                        parsed_url = urllib.parse.urlparse(item["link"])
                        real_src = parsed_url[0] + '://' + parsed_url[1] + real_src
                    elif real_src[real_src.find("http") + 4:].find("http") != -1:
                        real_src = real_src[real_src[(real_src.find("http") + 4):].find("http") + 4:]
                    images_list.append({"filename": filename, "src": real_src})
                except:
                    pass

            return {
                "images": images_list,
                "body": str(item_soup),
                "feedMode": "rss"
            }
        else:
            return {"feedMode": "web"}

    def process_response(self, response):
        result = []
        try:
            if not self._channel_record.get("notYahoo"):
                body = json_decode(response.body)

                if body["query"] and body["query"]["count"] > 0:
                    type = "rss"
                    items=[]
                    if "entry" in body["query"]["results"]:
                        items = body["query"]["results"]["entry"]
                        type = "atom"
                    if "item" in body["query"]["results"]:
                        items = body["query"]["results"]["item"]
                        type = "rss"

                    for i in range(len(items)):
                        item = items[i]
                        # if not link:
                        if type == "rss":
                            title = item["title"] if "title" in item else ""
                            if self._channel_record["_id"] in RSS_CHANNELS_LINK_FROM_ID:
                                link = item["guid"]["content"]
                            else:
                                link = item["origLink"] if "origLink" in item else item["link"]
                            description = item["description"] if "description" in item else ""
                            full_text = item["encoded"] if "encoded" in item else item["full-text"] if \
                                "full-text" in item else item["content"] if "content" in item else description
                            enclosure = item["enclosure"] if "enclosure" in item else ""
                            pubDate = item["pubDate"] if "pubDate" in item else None
                        else:
                            links = item["link"]
                            if isinstance(links, list):
                                link = item["link"][0]["href"] if "href" in item["link"][0] else ""
                            else:
                                link = item["link"]["href"] if "href" in item["link"] else ""
                            if isinstance(item["title"], str):
                                title = item["title"]
                            else:
                                title = item["title"]["content"] if "content" in item["title"] else ""

                            description = item["content"]["html"] if "content" in item and "html" in item[
                                "content"] else ""
                            full_text = description
                            pubDate = item["published"] if "published" in item else None

                        result.append(
                                {"link": link, "title": title, "full_text": full_text, "pubDate": 1000 * time.mktime(
                                        parse(pubDate).astimezone(tzutc()).timetuple()) if pubDate else int(time.time() * 1000)})
            else:
                soup = BeautifulSoup(response.body, features="xml")
                items = soup.find_all('item')
                for item in items:
                    result.append({"link": item.link.text, "title": item.title.text, "pubDate": 1000 * time.mktime(
                            parse(item.pubDate.text).astimezone(tzutc()).timetuple() if item.pubDate.text else int(time.time() * 1000)
                    )})
                else:
                    items = soup.find_all('entry')
                    for item in items:
                        link = ""
                        if isinstance(item.link.text, str) and item.link.text:
                            link = item.link.text
                        else:
                            link = item.link.attrs["href"] if hasattr(item.link,
                                                                      "attrs") and "href" in item.link.attrs else ""
                        if link:
                            result.append({"link": link, "title": item.title.text, "pubDate": 1000 * time.mktime(
                                    parse(item.published.text).astimezone(tzutc()).timetuple())})

            if result:
                result.sort(key=lambda x: x["pubDate"], reverse=False)

            return result[-(int(DEFAULT_CHANNEL_MAX_ARTICLES)):]
        except Exception as err:
            raise


class MobileSnapshot(object):

    def mark_success(self, channel_id):
        try:
            channel = channels_queue.find_one({"_id":channel_id})
        except NetworkTimeout:
            channel = channels_queue.find_one({"_id":channel_id})
        if channel:
            channel["lastImport"] = int(time.time() * 1000)
            try:
                channels_queue.replace_one({"_id": channel_id},channel)
            except NetworkTimeout:
                channels_queue.replace_one({"_id": channel_id},channel)

    def __init__(self, message, consumer):
        self._consumer = consumer
        self._attemptNumber = 0
        self._channel_type = consumer.channel_type
        self._client = consumer.http_client
        self._queue_type = consumer.queue_type
        self._message = message
        self._url = None
        self._channels_db = None
        self._channel_record = None
        self._channel_doc = None
        self.channel_db = None
        self._body = json_decode(message.body.decode('utf_8'))
        self.import_tag = self._body["importTag"]
        try:
            self.article_record = articles_queue.find_one({"importTag":self.import_tag})
        except NetworkTimeout:
            self.article_record = articles_queue.find_one({"importTag":self.import_tag})

        if self.article_record and self.article_record["status"] == 'active' and self.article_record["srcType"] != 'vk' and self.article_record["feedMode"] != "rss":
            self.channel_db = server[self.article_record["channelId"]]
            try:
                self.channel_mobile_db = server[self.article_record["channelId"] + '_m']
            except couchdb.http.ResourceNotFound:
                self.channel_mobile_db = server.create(self.article_record["channelId"] + '_m')
                self.channel_mobile_db.save({
                    "_id": "_design/articles",
                    "views": {
                        "articles": {
                            "map": "function(doc) {if(!doc.frozen&&doc.type=='article'){emit(doc.pubDate, doc._id);}}"
                        }
                    }
                })
            except:
                self.channel_mobile_db = server[self.article_record["channelId"] + '_m']

            self.articleId = self.article_record["articleId"]
            loggers[self._channel_type]["info"]["logger"].debug("taking mobile snapshot channel %s, article %s, tag %s",
                                                                self.article_record["channelId"],
                                                                self.article_record["articleId"],
                                                                self._body["importTag"])
            self.import_mobile_article(self.article_record)

        else:
            self._message.delete()


    def import_mobile_article(self, article):
        loggers[self._channel_type]["info"]["logger"].debug("import mobile article %s(%s) ,channel %s, mode %s",
                article["articleId"], article["importUrl"], article["channelName"],article.get("feedMode"))
        try:
            if article.get("feedMode") != "rss" or len(article.get("body","")) < 10:

                if article["channelId"] in ("c_habrahabr_all", "c_geektimes_all", "c_megamozg_all"):
                    import_url = url_fix(article.get("importUrl","").replace('http://', 'http://m.'))
                else:
                    import_url = url_fix(article.get("importUrl", ""))

                couch_mobile_doc = None

                try:
                    couch_mobile_doc = self.channel_mobile_db.get(article["articleId"])
                except:
                    couch_mobile_doc = self.channel_mobile_db.get(article["articleId"])

                filename = str(uuid4())

                if not couch_mobile_doc:
                    try:
                        couch_mobile_doc = self.channel_db.get(article["articleId"])
                        del couch_mobile_doc["_rev"]
                        if couch_mobile_doc.get("_attachments"):
                            del couch_mobile_doc["_attachments"]

                    except:
                        couch_mobile_doc = self.channel_db.get(article["articleId"])
                        del couch_mobile_doc["_rev"]
                        if couch_mobile_doc.get("_attachments"):
                            del couch_mobile_doc["_attachments"]
                # else:
                #     if ("attachments" in couch_mobile_doc and "webShot" in couch_mobile_doc[
                #         "attachments"] and "fileName" in
                #         couch_mobile_doc["attachments"]["webShot"]):
                #         filename = couch_mobile_doc["attachments"]["webShot"]["fileName"][0]


                if couch_mobile_doc:
                    if couch_mobile_doc.get("_attachments"):
                        attachments = couch_mobile_doc.get("_attachments")
                        try:
                            for name, info in attachments.items():
                                self.channel_mobile_db.delete_attachment(couch_mobile_doc, name)
                        finally:
                            couch_mobile_doc = self.channel_mobile_db.get(article["articleId"])


                    attachments = {}

                    i = 0
                    process=multiprocessing.Process(target=getScreenShot, args=(url_fix(import_url),filename,"mobile",self._channel_type,article.get("cookies",""),60))
                    process.start()
                    print("start")
                    process.join(90)
                    print("end")
                    filesize=0
                    try:
                        filesize=os.path.getsize(filename)
                    except:
                        pass
                    try:
                        process.terminate()
                    except Exception as err:
                        pass

                    if filesize < 15000:
                        time.sleep(15)
                        process=multiprocessing.Process(target=getScreenShot, args=(url_fix(import_url),filename,"mobile",self._channel_type,article.get("cookies",""),90))
                        process.start()
                        print("start")
                        process.join(180)
                        print("end")
                        try:
                            filesize=os.path.getsize(filename)
                        except:
                            pass
                        try:
                            process.terminate()
                        except Exception as err:
                            pass

                    if filesize >= 15000:
                        attachments["webShot"] = {
                            "type": "webShot",
                            "title": article.get("title"),
                            "text": "",
                            "fileName": [filename]
                        }
                        couch_mobile_doc["attachments"]=attachments
                        try:
                            _id,_rev = self.channel_mobile_db.save(couch_mobile_doc)
                        except:
                            _id,_rev = self.channel_mobile_db.save(couch_mobile_doc)
                        couch_mobile_doc["_rev"] = _rev



                        file = open(filename,"rb")
                        try:
                            result = self.channel_mobile_db.put_attachment(couch_mobile_doc, file, filename=filename, content_type="application/pdf")
                        except Exception as err:
                            print("second attempt to save attachment")
                            time.sleep(1)
                            result = self.channel_mobile_db.put_attachment(couch_mobile_doc, file, filename=filename, content_type="application/pdf")
                            print("second attempt to save attachment sucseeded")
                        file.close()
                        try:
                            os.remove(filename)
                        except:
                            pass

                        article["status"] = 'archived20'
                        try:
                            articles_queue.replace_one({"_id":article["_id"]},article)
                        except NetworkTimeout:
                            articles_queue.replace_one({"_id":article["_id"]},article)

                        loggers["rss"]["info"]["logger"].debug(
                                "imported mobile article %s(%s) ,channel %s, mode %s",
                                article["articleId"], article["importUrl"], article["channelName"],
                                article.get("feedMode"))

                        queue_name = "channels.replication.add"
                        message = {"channelId": article["channelId"], "status": "archived",
                                   "oldStatus": "archived20", "articleId": article["articleId"],
                                   "_id": article["_id"], "isMobile": True}
                        self._consumer.send_message(queue_name, message)
                        self.mark_success(article["channelId"])
                    else:
                        loggers[self._channel_type]["errors"]["logger"].error('too small file %s,%s,%s',
                                                                              url_fix(article["importUrl"]),filename,str(filesize))
                else:
                    article["status"] = 'missing20'
                    try:
                        articles_queue.replace_one({"_id": article["_id"]}, article)
                    except NetworkTimeout:
                        articles_queue.replace_one({"_id": article["_id"]}, article)
        except Exception as err:
            print(traceback.format_exc())
            loggers[self._channel_type]["errors"]["logger"].error(
                "error when import mobile article %s(%s) ,channel %s, mode %s",
                    article["articleId"], article["importUrl"], article["channelName"], article.get("feedMode"))
            loggers[self._channel_type]["errors"]["logger"].error(err)
            self._message.delete()
        else:
            self._message.delete()


class ProcessArticleImportRSS(ProcessArticleImport):

    def import_image(self, imageUrl, doc):
        response = self._client.fetch(imageUrl, **self._client_config)
        if response.error is not None:
            loggers[self._channel_type]["errors"]["logger"].debug('second attempt to download file %s for %s (%s)',
                                                                  imageUrl, doc["title"], doc["src"])
            loggers[self._channel_type]["errors"]["logger"].debug(response.error)
            time.sleep(3)
            response = self._client.fetch(imageUrl, **self._client_config)
        if response.error is not None:
            loggers[self._channel_type]["errors"]["logger"].debug('third attempt to download file %s for %s (%s)',
                                                                  imageUrl, doc["title"], doc["src"])
            loggers[self._channel_type]["errors"]["logger"].debug(response.error)
            time.sleep(5)
            response = self._client.fetch(imageUrl, **self._client_config)
        if response.error is not None:
            loggers[self._channel_type]["errors"]["logger"].debug('fourth attempt to download file %s for %s (%s)',
                                                                  imageUrl, doc["title"], doc["src"])
            loggers[self._channel_type]["errors"]["logger"].debug(response.error)
            time.sleep(10)
            response = self._client.fetch(imageUrl, **self._client_config)
        return response
    #
    # def get_page_shot(self, url, width=None, cookies=""):
    #     loggers[self._channel_type]["info"]["logger"].debug("taking the shot of %s", url)
    #     try:
    #         result = createScreenshot.render(url,"desktop",)
    # # ,cookies=cookies
    #     except Exception as err:
    #         result = None
    #         loggers[self._channel_type]["errors"]["logger"].error('can''t make the shot of %s', url)
    #         loggers[self._channel_type]["errors"]["logger"].error(err)
    #     if not result:
    #         try:
    #             result = None
    #             result = self.desktopScreener.render(url)
    #         except Exception as err:
    #             loggers[self._channel_type]["errors"]["logger"].error(
    #                     'can''t make the shot of %s', url)
    #             loggers[self._channel_type]["errors"]["logger"].error(err)
    #
    #     return result

    def import_article(self, article):
        loggers[self._channel_type]["info"]["logger"].debug("import article %s(%s) ,channel %s, mode %s",
                                                            article["articleId"], article["importUrl"],
                                                            article["channelName"], article.get("feedMode"))

        try:
            if article.get("feedMode") != "rss" or len(article.get("body","")) < 10:
                article["feedMode"] = "web"
                # widths=[1024,640]
                attachments = {}
                filename = str(uuid4())
                i = 0
                process=multiprocessing.Process(target=getScreenShot, args=(url_fix(article["importUrl"]),filename,"desktop",self._channel_type,article.get("cookies",""),60))
                process.start()
                print("start")
                process.join(90)
                print("end")
                filesize=0
                try:
                    process.terminate()
                except Exception as err:
                    pass

                try:
                    filesize=os.path.getsize(filename)
                except:
                    pass
                if filesize < 30000:
                    # time.sleep(15)
                    filename = str(uuid4())
                    process=multiprocessing.Process(target=getScreenShot, args=(url_fix(article["importUrl"]),filename,"desktop",self._channel_type,article.get("cookies",""),60))
                    process.start()
                    print("start")
                    process.join(90)
                    print("end")
                    try:
                        process.terminate()
                    except Exception as err:
                        pass

                    try:
                        filesize=os.path.getsize(filename)
                    except Exception as err:
                        pass
                if filesize >= 30000:
                    attachments["webShot"] = {
                        "type": "webShot",
                        "title": article.get("title"),
                        "text": "",
                        "fileName": [filename]
                    }
                    couch_doc = self.save_article(attachments)

                    file = open(filename,"rb")
                    try:
                        result = self._channel_db.put_attachment(couch_doc, file, filename=filename, content_type="application/pdf")
                    except Exception as err:
                        print("second attempt to save attachment")
                        time.sleep(1)
                        result = self._channel_db.put_attachment(couch_doc, file, filename=filename, content_type="application/pdf")
                        print("second attempt to save attachment sucseeded")
                    file.close()
                    try:
                        os.remove(filename)
                    except:
                        pass
                    self.commit_import()

                else:
                    loggers[self._channel_type]["errors"]["logger"].error('too small file %s,%s,%s',
                                                                              url_fix(article["importUrl"]),filename,str(filesize))
                    if self._message: self._message.delete()


            else:
                doc = self.save_article()
                self.commit_import()
        except Exception as err:
            print(traceback.format_exc())
            loggers[self._channel_type]["errors"]["logger"].error(
                    "error when import article %s(%s) ,channel %s, mode %s",
                    article["articleId"], article["importUrl"],
                    article["channelName"], article.get("feedMode"))

            loggers[self._channel_type]["errors"]["logger"].error(err)
            # self._message.unlock()


class AddChannelRSS(AddChannel):

    def get_update_data(self, body, channelData=None):
        update_data = {
            "feedMode": "web" if "feedMode" not in self._channel_doc else self._channel_doc["feedMode"]
        }
        self._channel_record["feedMode"] = update_data["feedMode"]
        try:
            soup = BeautifulSoup(body, features="xml")
            if hasattr(soup, 'channel') and soup.channel:
                update_data["feedType"] = "rss"
                self._channel_doc["feedType"] = "rss"
                self._channel_record["feedType"] = "rss"
                if soup.channel.title and soup.channel.title.text:
                    self._channel_record["originalName"] = soup.channel.title.text
                    self._channel_doc["originalName"] = soup.channel.title.text
                    update_data["originalName"] = soup.channel.title.text
                if soup.channel.link and soup.channel.link.text:
                    self._channel_record["srcUrlPublic"] = soup.channel.link.text
                    self._channel_doc["srcUrlPublic"] = soup.channel.link.text
                    update_data["srcUrlPublic"] = soup.channel.link.text
                if soup.channel.description and soup.channel.description.text:
                    self._channel_record["originalDescription"] = soup.channel.description.text
                    self._channel_doc["originalDescription"] = soup.channel.description.text
                    update_data["originalDescription"] = soup.channel.description.text
                if soup.channel.image and soup.channel.image.url and soup.channel.image.url.text:
                    self._channel_record["imageUrl"] = soup.channel.image.url.text
                    update_data["imageUrl"] = soup.channel.image.url.text


            elif hasattr(soup, 'feed') and soup.feed:
                update_data["feedType"] = "atom"
                self._channel_doc["feedType"] = "atom"
                self._channel_record["feedType"] = "atom"
                if hasattr(soup.feed, 'title') and soup.feed.title:
                    if isinstance(soup.feed.title.text, str) and soup.feed.title.text:
                        title = soup.feed.title.text
                    else:
                        title = soup.feed.title if hasattr(soup.feed.title, "content") else ""
                    self._channel_record["originalName"] = title
                    self._channel_doc["originalName"] = title
                    update_data["originalName"] = title

                if hasattr(soup.feed, 'subtitle') and soup.feed.subtitle:
                    if isinstance(soup.feed.subtitle.text, str) and soup.feed.subtitle.text:
                        subtitle = soup.feed.subtitle.text
                    else:
                        subtitle = soup.feed.subtitle if hasattr(soup.feed.title, "content") else ""

                    self._channel_record["originalDescription"] = subtitle
                    self._channel_doc["originalDescription"] = subtitle
                    update_data["originalDescription"] = subtitle

                if hasattr(soup.feed, 'link') and soup.feed.link:
                    if isinstance(soup.feed.link.text, str) and soup.feed.link.text:
                        link = soup.feed.link.text
                    else:
                        link = soup.feed.link.attrs["href"] if hasattr(soup.feed.link,
                                                                       "attrs") and "href" in soup.feed.link.attrs else ""

                    self._channel_record["srcUrlPublic"] = link
                    self._channel_doc["srcUrlPublic"] = link
                    update_data["srcUrlPublic"] = link

                if hasattr(soup.feed, 'logo'):
                    if soup.feed.logo and isinstance(soup.feed.logo.text, str):
                        logo = soup.feed.logo.text
                        self._channel_record["imageUrl"] = logo
                        update_data["imageUrl"] = logo

            if channelData:
                if channelData.get("feedType"):
                    self._channel_doc["feedType"] = channelData["feedType"]
                    self._channel_record["feedType"] = channelData["feedType"]
                if channelData.get("name"):
                    self._channel_record["originalName"] = channelData["name"]
                    self._channel_doc["originalName"] = channelData["name"]
                    update_data["originalName"] = channelData["name"]
                if channelData.get("link"):
                    self._channel_record["srcUrlPublic"] = channelData["link"]
                    self._channel_doc["srcUrlPublic"] = channelData["link"]
                    update_data["srcUrlPublic"] = channelData["link"]
                if channelData.get("description"):
                    self._channel_record["originalDescription"] = channelData["description"]
                    self._channel_doc["originalDescription"] = channelData["description"]
                    update_data["originalDescription"] = channelData["description"]
                if channelData.get("imageUrl"):
                    self._channel_record["imageUrl"] = channelData["imageUrl"]
                    update_data["imageUrl"] = channelData["imageUrl"]
            try:
                _id, _rev = self._channels_db.save(self._channel_doc)
            except:
                _id, _rev = self._channels_db.save(self._channel_doc)
            self._channel_doc["_rev"]=_rev
            try:
                channels_queue.replace_one({"_id": self._channel_record["_id"]},self._channel_record)
            except NetworkTimeout:
                channels_queue.replace_one({"_id": self._channel_record["_id"]},self._channel_record)

        except Exception as err:
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(
                    "Failed to update data for channel %s (%s)",
                    self._channel_record["name"] or self._channel_record.get("originalName"),
                    self._channel_record["_id"])
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(err)
            return None
        return update_data


class ConsumerAddRSS(ConsumerAdd):
    def queue_callback(self, message):
        try:
            body = json_decode(message.body.decode('utf_8'))
            AddChannelRSS(body, self)
        except Exception as err:
            body = json_decode(message.body.decode('utf_8'))
            loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                               body["_id"],
                                                               body["importTag"], body["mode"])
            loggers[self.channel_type]["errors"]["logger"].error(err)
        finally:
            message.delete()


class ConsumerProcessChannelImportRSS(ConsumerProcessChannelImport):

    def queue_callback(self, message):
        try:
            ProcessChannelImportRSS(message, self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerProcessArticleImportRSS(ConsumerProcessArticleImport):

    def queue_callback(self, message):
        try:
            ProcessArticleImportRSS(message, self)
        except Exception as err:
            try:
                body = json_decode(message.body.decode('utf_8'))
                loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                                   body["_id"],
                                                                   body["importTag"], body["mode"])
                loggers[self.channel_type]["errors"]["logger"].error(err)
            finally:
                message.unlock()


class ConsumerProcessArticleImportMobileRSS(BaseConsumer):
    def __init__(self, channel_type, queue_type, http_client):
        super().__init__(queue_type)
        self.channel_type = channel_type
        self.queue_type = queue_type
        self.channel_type = channel_type
        self.http_client = http_client


    def empty_callback(self):
        channels_db = server["channels"]

    def queue_callback(self, message):
        try:
            MobileSnapshot(message, self)
        except Exception as err:
            body = json_decode(message.body.decode('utf_8'))
            loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                               body["_id"],
                                                               body["importTag"], body["mode"])
            loggers[self.channel_type]["errors"]["logger"].error(err)
            message.unlock()