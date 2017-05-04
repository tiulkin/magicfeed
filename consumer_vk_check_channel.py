from consumer_vk import *

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    client = httpclient.HTTPClient(defaults=dict({"connect_timeout": 120, "request_timeout": 120}))
    client_config = CLIENT_CONFIG
    consumer = ConsumerAddVK("vk","channels.vk.add",client)
    consumer.start_consuming()