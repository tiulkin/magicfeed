# !/usr/bin/env python3
import pdfkit
import io
from consumer_rss import *


class AddChannelLJ(AddChannelRSS):
    def get_url(self):
        try:
            if "srcUrl" in self._request_doc["channelData"]:
                href = self._request_doc["channelData"]["srcUrl"]
            else:
                href = CHANNEL_TYPE_URL_PARTS["lj"]["urlPrefix"] + self._request_doc["channelData"]["url"] + \
                       CHANNEL_TYPE_URL_PARTS["lj"]["urlSuffix"]
            tldextract.extract(href)
            url = urllib.parse.urlparse(href)
            href += '/data/rss'

            return href

        except Exception as err:
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(
                    "Check url %s for request %s",
                    self._request_doc["channelData"]["url"], self._request_doc["_id"])
            loggers[self._request_doc["channelData"]["sourceType"]]["errors"]["logger"].error(err)
            return None

class ConsumerAddLJ(ConsumerAdd):
    def queue_callback(self, message):
        try:
            body = json_decode(message.body.decode('utf_8'))
            AddChannelLJ(body, self)
        except Exception as err:
            message.unlock()
            loggers[self.channel_type]["errors"]["logger"].error("Check channel %s with tag %s,mode '%s'",
                                                               body["_id"],
                                                               body["importTag"], body["mode"])
            loggers[self.channel_type]["errors"]["logger"].error(err)
        else:
            message.delete()

