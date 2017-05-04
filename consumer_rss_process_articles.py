from consumer_rss import *

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    multiprocessing.set_start_method('spawn')
    client = httpclient.HTTPClient(defaults=dict({"connect_timeout": 120, "request_timeout": 120}))
    client_config = CLIENT_CONFIG
    consumer = ConsumerProcessArticleImportRSS("rss","channels.rss.items",client)
    consumer.start_consuming()