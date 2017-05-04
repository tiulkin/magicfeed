import time
import os
import sys
import io
import sip
import six
import logging
import signal
from urllib import parse
from uuid import uuid4
from PyQt4.QtCore import *
from loggers import loggers
from PyQt4.QtGui import *
# from PyQt4.QtWidgets import *
# from PyQt4.QtWebKitWidgets import *
from PyQt4.QtWebKit import *
# from PyQt4.QtPrintSupport import *
from PyQt4.QtWebKit import QWebSettings
# from PyQt5.QtWebPage import *
from PyQt4.QtNetwork import *
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import *
from multiprocessing import Queue, Process
from subprocess import call

renderHints={
    "news.mail.ru":{
        "classes":"kkmr-close"
    },
    "adme.ru":{
        "cookies":{
            "social.overlay.fb.status": "disabled",
            "social": "1"
        }
    }

}

Signal = pyqtSignal
Slot = pyqtSlot
# LOG_FILENAME = 'webkit2x.log'
# logger = logging.getLogger('webkit2x')

class WebkitRenderer(QObject):
    def __init__(self,**kwargs):
        if not QApplication.instance():
            raise RuntimeError(self.__class__.__name__ + " requires a running QApplication instance")
        QObject.__init__(self)

        # Initialize default properties
        self.width = kwargs.get('width', 0)
        self.kind = kwargs.get('kind', 'desktop')
        self.height = kwargs.get('height', 0)
        self.dpi = kwargs.get('dpi', 96)
        self.timeout = kwargs.get('timeout', 0)
        self.wait = kwargs.get('wait', 0)
        self.scaleToWidth = kwargs.get('scaleToWidth', 0)
        self.scaleToHeight = kwargs.get('scaleToHeight', 0)
        self.scaleRatio = kwargs.get('scaleRatio', 'keep')
        self.format = kwargs.get('format', 'png')
        self.logger = kwargs.get('logger', None)
        self.grabWholeWindow = kwargs.get('grabWholeWindow', False)
        self.renderTransparentBackground = kwargs.get('renderTransparentBackground', False)
        self.ignoreAlert = kwargs.get('ignoreAlert', True)
        self.ignoreConfirm = kwargs.get('ignoreConfirm', True)
        self.ignorePrompt = kwargs.get('ignorePrompt', True)
        self.interruptJavaScript = kwargs.get('interruptJavaScript', True)
        self.encodedUrl = kwargs.get('encodedUrl', False)
        self.cookies = kwargs.get('cookies', {})
        self.afterRender=kwargs.get('afterRender',{})
        self.qWebSettings = {
            QWebSettings.JavascriptEnabled : True,
            QWebSettings.PluginsEnabled : True,
            QWebSettings.PrivateBrowsingEnabled : False,
            QWebSettings.JavascriptCanOpenWindows : False
        }


    def render(self,res,fileName=""):
        self.logger.debug("create _WebkitRendererHelper")
        helper = _WebkitRendererHelper(self)
        self.logger.debug("createed _WebkitRendererHelper")
        helper._window.resize(self.width, self.height )
        result = helper.render(res,fileName)
        # helper._window.close()
        return result

## @brief The CookieJar class inherits QNetworkCookieJar to make a couple of functions public.
class CookieJar(QNetworkCookieJar):
    def __init__(self, cookies, qtUrl, parent=None):
        QNetworkCookieJar.__init__(self, parent)
        encodedCookies=[]
        cookies =None
        if cookies:
            for cookie in cookies:
                encodedCookies.append(QNetworkCookie(QByteArray(cookie),QByteArray(cookies[cookie])))
        QNetworkCookieJar.setCookiesFromUrl(self, encodedCookies, qtUrl)

    def allCookies(self):
        return QNetworkCookieJar.allCookies(self)

    def setAllCookies(self, cookieList):
        QNetworkCookieJar.setAllCookies(self, cookieList)

class _WebkitRendererHelper(QObject):

    def __init__(self, parent):
        QObject.__init__(self)

        for key,value in parent.__dict__.items():
            setattr(self,key,value)

        proxy = QNetworkProxy(QNetworkProxy.NoProxy)
        if 'http_proxy' in os.environ:
            proxy_url = QUrl(os.environ['http_proxy'])
            if proxy_url.scheme().startswith('http'):
                protocol = QNetworkProxy.HttpProxy
            else:
                protocol = QNetworkProxy.Socks5Proxy

            proxy = QNetworkProxy(
                protocol,
                proxy_url.host(),
                proxy_url.port(),
                proxy_url.userName(),
                proxy_url.password()
            )

        self._page = CustomWebPage(logger=self.logger, ignore_alert=self.ignoreAlert,
            ignore_confirm=self.ignoreConfirm, ignore_prompt=self.ignorePrompt,
            interrupt_js=self.interruptJavaScript,kind=self.kind)
        self._page.networkAccessManager().setProxy(proxy)
        self._page.settings().setAttribute(QWebSettings.DeveloperExtrasEnabled, True)
        self._page.mainFrame().setScrollBarPolicy(Qt.Horizontal, Qt.ScrollBarAlwaysOff)
        self._page.mainFrame().setScrollBarPolicy(Qt.Vertical, Qt.ScrollBarAlwaysOff)
        # self._page.kind=self.kind
        self._view = QWebView()
        self._view.setPage(self._page)
        self._window = QMainWindow()
        self._window.setCentralWidget(self._view)
        for key, value in self.qWebSettings.items():
            self._page.settings().setAttribute(key, value)

        self._view.loadFinished.connect(self._on_load_finished)
        self._view.loadStarted.connect(self._on_load_started)
        # self._page.networkAccessManager().finished.connect(self._on_each_reply)
        self._page.networkAccessManager().sslErrors.connect(self._on_ssl_errors)

        self._page.settings().setUserStyleSheetUrl(QUrl("data:text/css,html,body{overflow-y:hidden !important;}"))

        self._window.show()

    def __del__(self):
        """
        Clean up Qt4 objects.
        """
        if hasattr(self,"_window"):
            self._window.close()
            del self._window
            del self._view
            del self._page

    def render(self, res, fileName):
        print(self.width)
        pdfOnly=True
        pngOnly=False
        # if fileName[-3:]=="pdf":
        #     pdfOnly=True
        # if fileName[-3:]=="png":
        #     pngOnly=True
        time1=time.time()
        self._page.setViewportSize(QSize(self.width, self.height))
        self._load_page(res, self.width, self.height, self.timeout)
        self.logger.debug("загрузка %s",str(time.time()-time1))
        time1=time.time()
        if self.wait > 0:
            if self.logger: self.logger.debug("Waiting %d seconds " % self.wait)
            waitToTime = time.time() + self.wait
            while time.time() < waitToTime:
                time.sleep(1)
                if QApplication.hasPendingEvents():
                    QApplication.processEvents()
        self.logger.debug("подождали %s",str(time.time()-time1))
        time1=time.time()
        # self._page.setFixedWidth(self.width)
        size = self._page.mainFrame().contentsSize()
        self.logger.debug(size)
        newsize=QSize(self.width, size.height())
        self._page.setViewportSize(size)
        self._view.resize(size)
        if fileName and not pdfOnly:
            image = QPixmap.grabWidget(self._view)
            qBuffer = QBuffer()
            image.save(qBuffer, 'png')
            if os.path.exists(fileName+("" if pngOnly else ".png")):
                os.remove(fileName+("" if pngOnly else ".png"))
            open(fileName+("" if pngOnly else ".png"), "wb").write(qBuffer.buffer().data())

        # self._window.resize(newsize)
        if pngOnly:
            return fileName

        painter = QPainter()
        # painter.setDevicePixelRatio(3)
        self._printer = QPrinter(QPrinter.ScreenResolution)
        self._printer.setPaperSize(QSizeF(size),QPrinter.DevicePixel)
        self._printer.setOutputFormat(QPrinter.PdfFormat)
        # self._printer.printEngine.PPK_ImageQuality=80
        self._printer.setOrientation(QPrinter.Portrait)
        # self._printer.setResolution(self.winDpi)
        self._printer.setFullPage(True)

        self._printer.setFontEmbeddingEnabled(True)
        self._printer.setPageMargins(0,0,0,0,QPrinter.Millimeter)
        tmpFileName=str(uuid4())
        self._printer.setOutputFileName(tmpFileName)
        self._printer.setCreator("MagicFeed")
        r=False
        x=0
        while not r:
            if x>10:
                QApplication.exit()
                return None
            print(x)
            x+=1
            r=painter.begin(self._printer)
            time.sleep(1)
        self.logger.debug("рендер начат  %s",str(time.time()-time1))
        time1=time.time()
        self._page.mainFrame().render(painter)
        self.logger.debug("рендер окончен  %s",str(time.time()-time1))
        time1=time.time()
        # if self.wait > 0:
        #     if self.logger: self.logger.debug("Waiting %d seconds " % self.wait)
        #     waitToTime = time.time() + self.wait
        #     while time.time() < waitToTime:
        #         time.sleep(1)
        #         if QApplication.hasPendingEvents():
        #             QApplication.processEvents()
        painter.save()
        painter.end()

        links=[]
        location=self._page.mainFrame().baseUrl()
        self.logger.debug("поиск ссылок  %s",str(time.time()-time1))
        time1=time.time()
        linkElements = self._page.mainFrame().findAllElements("a")
        for link in linkElements:
            if(link.geometry().width() and link.geometry().height()):
                rect=(link.geometry().bottomLeft().x()*inch/96,link.geometry().bottomLeft().y()*inch/96,
                      link.geometry().width()*inch/96,link.geometry().height()*inch/96)
                url=link.attribute("href")
                if url and url[:10]!="javascript":
                    if len(url)>1 and url[0]=="/" and url[1]=="/":
                        url="http:"+url
                    elif url[0]=="/":
                        # print(location.host()+url)
                        url=location.scheme()+"://"+location.host()+url
                    links.append((url,rect,"link"))

        linkElements = self._page.mainFrame().findAllElements("iframe")
        for link in linkElements:
            if(link.geometry().width() and link.geometry().height()):
                rect=(link.geometry().bottomLeft().x()*inch/96,link.geometry().bottomLeft().y()*inch/96,
                      link.geometry().width()*inch/96,link.geometry().height()*inch/96)
                url=link.attribute("src")
                if url:
                    if len(url)>1 and url[0]=="/" and url[1]=="/":
                        url="http:"+url
                    elif url[0]=="/":
                        # print(location.host()+url)
                        url=location.scheme()+"://"+location.host()+url
                    links.append((url,rect,"youtube"))
        if self._window:
            self._window.close()
        del self._window
        del self._view
        del self._page

        self.logger.debug("добавление ссылок  %s",str(time.time()-time1))
        time1=time.time()
        result = self.add_links((size.width()*inch/96,size.height()*inch/96),links,tmpFileName,fileName)
        # print(tmpFileName)
        try:
            os.remove(tmpFileName)
        except Exception as err:
            self.logger.debug(err)
        self.logger.debug("готово  %s",str(time.time()-time1))
        return result

    def add_links(self, size, links,inputFileName, fileName=None):
        stream = open(inputFileName+".links","wb")
        newCanvas = canvas.Canvas(stream, pagesize=(size[0],size[1]),bottomup=1,pageCompression=1,verbosity=0)
        time1=time.time()
        for url,rect,kind in links:
            if rect[0]>=0 and rect[1]>=0 and rect[2]>0 and rect[3]>0:
                newCanvas.linkURL(url=url, rect=(rect[0],size[1]-rect[1],rect[0]+rect[2],size[1]-rect[1]+rect[3]), relative=0)
        self.logger.debug("Ссылки  %s",str(time.time()-time1))
        time1=time.time()
        newCanvas.rect(0,0,1,1)
        newCanvas.save()
        stream.close()
        # stream.seek(0)
        # new_pdf = pyPdf.PdfFileReader(stream,overwriteWarnings=True)
        # existing_pdf = pyPdf.PdfFileReader(open(inputFileName, "rb"),overwriteWarnings=True)
        # output = pyPdf.PdfFileWriter()
        # page = existing_pdf.getPage(0)
        # page.mergePage(new_pdf.getPage(0))
        # page.compressContentStreams()
        # output.addPage(page)
        # if fileName:
        fileExt="" #".pdf" if fileName[-3:]!="pdf" else ""
        # outputStream = open(fileName+fileExt+".tmp", "wb")
        # output.compressContentStreams()
        # output.write(outputStream)
        # outputStream.close()
        try:
            retcode = call(['gs',"-sDEVICE=pdfwrite","-dNOPAUSE","-dQUIET", "-dBATCH", "-sOutputFile="+fileName+fileExt+'.tmp',inputFileName])
            if retcode < 0:
                print ("Child was terminated by signal", -retcode)
        except OSError as e:
            print (sys.stderr, "Execution failed:", e)
        else:
            try:
                try:
                    os.remove(fileName+fileExt)
                except Exception as err:
                    pass

                retcode = call(['pdftk',inputFileName+".links","multistamp",fileName+fileExt+'.tmp', "output",fileName+fileExt])

                if retcode < 0:
                    print ("Child was terminated by signal", -retcode)
            except OSError as e:
                print (sys.stderr, "Execution failed:", e)
            finally:
                try:
                    os.remove(fileName+fileExt+'.tmp')
                except Exception as err:
                    self.logger.error(err)
        finally:
            try:
                os.remove(inputFileName+".links")
            except Exception as err:
                self.logger.error(err)


        self.logger.debug("Файлы  %s",str(time.time()-time1))
        return fileName+fileExt


    def _load_page(self, res, width, height, timeout):
        cancelAt = time.time() + timeout
        self.__loading = True
        self.__loadingResult = False # Default
        if type(res) == tuple:
            url = res[1]
        else:
            url = res

        # if self.encodedUrl:
        qtUrl = QUrl.fromEncoded(url)
        # else:
        #     qtUrl = QUrl(url)

        # Set the required cookies, if any
        if not self.cookies:
            self.cookies=renderHints.get(qtUrl.host(),{}).get("cookies")

        self.cookieJar = CookieJar(self.cookies, qtUrl)
        self._page.networkAccessManager().setCookieJar(self.cookieJar)

        # Load the page
        if type(res) == tuple:
            self._page.mainFrame().setHtml(res[0], qtUrl) # HTML, baseUrl
        else:
            self._page.mainFrame().load(qtUrl)

        while self.__loading:
            time.sleep(1)
            if timeout > 0 and time.time() >= cancelAt:
                # self.view.stop()
                # time.sleep(1)
                break
                # raise RuntimeError("Request timed out on %s" % res)
            while QApplication.hasPendingEvents():
                QCoreApplication.processEvents()

        if self.logger: self.logger.debug("Processing result")
        js_scroll = "window.scrollBy(0, 5000);setTimeout(3,function(){window.scrollBy(0, 0)})"
        self._page.mainFrame().documentElement().evaluateJavaScript(js_scroll)

        if not self.afterRender:
            self.afterRender=renderHints.get(qtUrl.host())
        if self.afterRender:
            js = """
                alert("start")
                function eventFire(el, etype){
                  if (el.fireEvent) {
                    el.fireEvent('on' + etype);
                  } else {
                    var evObj = document.createEvent('Events');
                    evObj.initEvent(etype, true, false);
                    el.dispatchEvent(evObj);
                  }
                }
            """

            if self.afterRender.get("classes"):
                js = js+"var elements = document.getElementsByClassName('"+self.afterRender["classes"]+"');for (var i=0; i<elements.length; i++)eventFire(elements[i],'click');"
            if self.afterRender.get("id"):
                js = js+"eventFire(document.getElementById('"+self.afterRender["id"]+"'),'click')"
            if self.afterRender.get("js"):
                js = js + self.afterRender.get("js")


            self._page.mainFrame().documentElement().evaluateJavaScript(js)


        # if self.__loading_result == False:
        #     if self.logger: self.logger.warning("Failed to load %s" % res)


        # Set initial viewport (the size of the "window")
        size = self._page.mainFrame().contentsSize()
        if self.logger: self.logger.debug("contentsSize: %s", size)
        # if width > 0:
        #     size.setWidth(width)
        # if height > 0:
        #     size.setHeight(height)
        # self._window.resize(size)

    # @pyqtSlot()
    def _on_each_reply(self,reply):
      """
      Logs each requested uri
      """
      # print("Received %s" % (reply.url().toString()))
      # self.logger.debug("Received %s" % (reply.url().toString()))

    # Eventhandler for "loadStarted()" signal
    # @pyqtSlot()
    def _on_load_started(self):
        """
        Slot that sets the '__loading' property to true
        """
        if self.logger: self.logger.debug("loading started")
        self.__loading = True

    # Eventhandler for "loadFinished(bool)" signal
    # @pyqtSlot()
    def _on_load_finished(self, result):
        """Slot that sets the '__loading' property to false and stores
        the result code in '__loading_result'.
        """
        if self.logger: self.logger.debug("loading finished with result %s", result)
        self.__loading = False
        self.__loading_result = result

    # Eventhandler for "sslErrors(QNetworkReply *,const QList<QSslError>&)" signal
    # @pyqtSlot()
    def _on_ssl_errors(self, reply, errors):
        """
        Slot that writes SSL warnings into the log but ignores them.
        """
        # for e in errors:
        #     if self.logger: self.logger.warn("SSL: " + e.errorString())
        reply.ignoreSslErrors()


class CustomWebPage(QWebPage):
    def __init__(self, **kwargs):
        super(CustomWebPage, self).__init__()
        self.logger = kwargs.get('logger', None)
        self.ignore_alert = kwargs.get('ignore_alert', True)
        self.ignore_confirm = kwargs.get('ignore_confirm', True)
        self.ignore_prompt = kwargs.get('ignore_prompt', True)
        self.interrupt_js = kwargs.get('interrupt_js', True)
        self.kind=kwargs.get('kind', "mobile")

    def javaScriptAlert(self, frame, message):
        if self.logger: self.logger.debug('Alert: %s', message)
        if not self.ignore_alert:
            return super(CustomWebPage, self).javaScriptAlert(frame, message)

    def javaScriptConfirm(self, frame, message):
        if self.logger: self.logger.debug('Confirm: %s', message)
        if not self.ignore_confirm:
            return super(CustomWebPage, self).javaScriptConfirm(frame, message)
        else:
            return False

    def javaScriptPrompt(self, frame, message, default, result):
        if self.logger: self.logger.debug('Prompt: %s (%s)' % (message, default))
        if not self.ignore_prompt:
            return super(CustomWebPage, self).javaScriptPrompt(frame, message, default, result)
        else:
            return False

    def shouldInterruptJavaScript(self):
        if self.logger: self.logger.debug("WebKit ask to interrupt JavaScript")
        return self.interrupt_js


    def userAgentForUrl(self, url):
        if self.kind=="mobile":
            return "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3 like Mac OS X) AppleWebKit/537.21 (KHTML, like Gecko) Version/9.0 Mobile/13E230 Safari/537.21"
        else:
            return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/601.4.4 (KHTML, like Gecko) Version/9.0.3 Safari/601.4.4"

    # loadFinished signal handler receives ok=False at least these cases:
    # 1. when there is an error with the page (e.g. the page is not available);
    # 2. when a redirect happened before all related resource are loaded;
    # 3. when page sends headers that are not parsed correctly
    #    (e.g. a bad Content-Type).
    # By implementing ErrorPageExtension we can catch (1) and
    # distinguish it from (2) and (3).
    # def extension(self, extension, info=None, errorPage=None):
    #     if extension == QWebPage.ErrorPageExtension:
    #         # catch the error, populate self.errorInfo and return an error page
    #         print(1)
    #         info = sip.cast(info, QWebPage.ErrorPageExtensionOption)
    #
    #         domain = 'Unknown'
    #         if info.domain == QWebPage.QtNetwork:
    #             domain = 'Network'
    #         elif info.domain == QWebPage.Http:
    #             domain = 'HTTP'
    #         elif info.domain == QWebPage.WebKit:
    #             domain = 'WebKit'
    #
    #         self.error_info = RenderErrorInfo(
    #             domain,
    #             int(info.error),
    #             six.text_type(info.errorString),
    #             six.text_type(info.url.toString())
    #         )
    #
    #         # XXX: this page currently goes nowhere
    #         content = u"""
    #             <html><head><title>Failed loading page</title></head>
    #             <body>
    #                 <h1>Failed loading page ({0.text})</h1>
    #                 <h2>{0.url}</h2>
    #                 <p>{0.type} error #{0.code}</p>
    #             </body></html>""".format(self.error_info)
    #
    #         errorPage = sip.cast(errorPage, QWebPage.ErrorPageExtensionReturn)
    #         errorPage.content = QByteArray(content.encode('utf-8'))
    #         return True
    #
    #     # XXX: this method always returns True, even if we haven't
    #     # handled the extension. Is it correct? When can this method be
    #     # called with extension which is not ErrorPageExtension if we
    #     # are returning False in ``supportsExtension`` for such extensions?
    #     return True

    # def supportsExtension(self, extension):
    #     if extension == QWebPage.ErrorPageExtension:
    #         return True
    #     return False

#
# class Screener():
#     def __init__(self,kind):
#         # self.queue = Queue(1)
#         super(Screener, self).__init__()
#         # logging.basicConfig(filename=LOG_FILENAME,level=logging.WARN,)
#         self.app=self.init_qtgui(":100") if kind == "mobile" else self.init_qtgui(":101")
#         signal.signal(signal.SIGINT, signal.SIG_DFL)
#         self.renderer = self.createRenderer(kind)
#     def render(self,url,fileName):
#         result = None
#         try:
#             result = self.renderer.render(res=url, fileName=fileName)
#             # self.queue.put(result)
#         except Exception as err:
#             logger.debug("err")
#             print(err)
#         return result
#     def __del__(self):
#         QApplication.exit(0)
#
#         # QTimer.singleShot(0, __main_qt)
#         # return app.exec_()
#
#     def init_qtgui(self,display=None, style=None, qtargs=None):
#         if QApplication.instance():
#             logger.debug("QApplication has already been instantiated. \
#                             Ignoring given arguments and returning existing QApplication.")
#             return QApplication.instance()
#
#         qtargs2 = [sys.argv[0]]
#
#         if display:
#             qtargs2.append('-display')
#             qtargs2.append(display)
#             # Also export DISPLAY var as this may be used
#             # by flash plugin
#             os.environ["DISPLAY"] = display
#         # os.environ["QTWEBKIT_DEVICE_WIDTH"]="360"
#         # os.environ["QTWEBKIT_DEVICE_HEIGHT"]="640"
#         # os.environ['QT_DEVICE_PIXEL_RATIO'] = str( int(2) )
#         # os.environ['QT_HIGHDPI_SCALE_FACTOR'] = str(int(2))
#         # os.environ['QT_AUTO_SCREEN_SCALE_FACTOR'] = str(int(0))
#         # os.environ['QT_SCALE_FACTOR'] = str(int(2))
#         qtargs2.extend(qtargs or [])
#
#         return QApplication(qtargs2)
#
#     def createRenderer(self,kind):
#         # Render the page.
#         # If this method times out or loading failed, a
#         # RuntimeException is thrown
#         try:
#             cookies=""
#             renderer = WebkitRenderer(kind=kind)
#             # url="https://habrahabr.ru/post/302694/"
#             # url="http://arzamas.academy/materials/530"
#             # url= "http://sergeydolya.livejournal.com/1227195.html"
#             # fileName="test1.pdf"
#             renderer.logger = logger
#             renderer.kind=kind
#             renderer.width = 375 if kind == "mobile" else 1136
#             renderer.height = 559 if kind == "mobile" else 800
#             renderer.dpi = 96
#             renderer.timeout = 60
#             renderer.wait = 2
#             renderer.format = "pdf"
#             # renderer.encodedUrl = options.encoded_url
#             if cookies:
#                 renderer.cookies = cookies
#             renderer.qWebSettings[QWebSettings.JavascriptEnabled] = True
#             renderer.qWebSettings[QWebSettings.PluginsEnabled] = True
#             renderer.qWebSettings[QWebSettings.PrivateBrowsingEnabled] = True
#             renderer.qWebSettings[QWebSettings.JavascriptCanOpenWindows] = False
#             return renderer
#
#         except RuntimeError as e:
#             logger.error("main: %s" % e)
#             print (e)
#             # QApplication.exit(1)
#             # QApplication.exit(1)
#

def createScreenshot(url,fileName,mode,logger,cookies="",timeout=50):
    qtargs2 = []
    qtargs2.append('-display')
    display=":100" if mode == "mobile" else ":101"
    qtargs2.append(display)
    os.environ["DISPLAY"] = display
    app=QApplication(qtargs2)
    # logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,)
    renderer = WebkitRenderer(kind=mode,logger=loggers[logger]["info"]["logger"])
    renderer.logger = loggers[logger]["info"]["logger"]
    renderer.kind=mode
    renderer.width = 375 if mode == "mobile" else 1136
    renderer.height = 559 if mode == "mobile" else 800
    renderer.dpi = 96
    renderer.timeout = timeout
    renderer.wait = 2
    renderer.format = "pdf"
    # renderer.encodedUrl = options.encoded_url
    if cookies:
        renderer.cookies = cookies
    renderer.qWebSettings[QWebSettings.JavascriptEnabled] = True
    renderer.qWebSettings[QWebSettings.PluginsEnabled] = True
    renderer.qWebSettings[QWebSettings.PrivateBrowsingEnabled] = False
    renderer.qWebSettings[QWebSettings.JavascriptCanOpenWindows] = False

    def renderToFile():
        print("render1")
        result=renderer.render(res=url, fileName=fileName)
        if result:
            print("rendered")
            QApplication.exit(0)
        else:
            QApplication.exit(1)
    print("render")
    QTimer.singleShot(0, renderToFile)
    return app.exec_()

