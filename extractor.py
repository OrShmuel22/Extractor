import logging
from InterfaceABS.MicroServiceABS import Microservice
from utils.rabbitqAdapter import RabbitMQMessageBroker


class MyExtractor(Microservice):
    def __init__(self, message_broker):
        self.message_broker = message_broker
        # Todo get it from configuration file with different types of extractor we have
        self.extractors = {}

    def start(self):
        self.message_broker.connect()
        logging.info('Connected to message broker')

    def stop(self):
        self.message_broker.disconnect()
        logging.info('Disconnected from message broker')

    def execute(self, message):
        # Add your message extraction logic here
        logging.info(f"Received file: {message}")


if __name__ == '__main__':
    message_broker = RabbitMQMessageBroker('localhost', 5672, 'guest', 'guest', routing_key='extractor')
    extractor = MyExtractor(message_broker)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    try:
        extractor.start()
        extractor.message_broker.subscribe(callback=extractor.execute, topic='extractor')
    except KeyboardInterrupt:
        extractor.stop()
