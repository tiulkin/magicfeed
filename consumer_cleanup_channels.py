from consumer_rss import *

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    consumer = ConsumerProcessChannelCleanup("clean","channels.clean.custom_1")
    consumer.start_consuming()