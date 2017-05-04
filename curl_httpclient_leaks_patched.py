__author__ = 'oleg'
from tornado import curl_httpclient
class CurlAsyncHTTPClientEx(curl_httpclient.CurlAsyncHTTPClient):
    def close(self):
        super(CurlAsyncHTTPClientEx, self).close()
        del self._multi