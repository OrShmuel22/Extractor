import logging
from utils.rabbitqAdapter import RabbitMQMessageBroker

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    message_broker = RabbitMQMessageBroker('localhost', 5672, 'guest', 'guest', routing_key='extractor')
    try:
        message_broker.connect()
        logging.info('Connected to message broker')
        message = 'example_file.txt'
        message_broker.publish(message, 'extractor')
        logging.info(f"Published file {message} to extractor queue")
    finally:
        message_broker.disconnect()
        logging.info('Disconnected from message broker')
