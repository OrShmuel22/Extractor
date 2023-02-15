import os
import glob
import logging

from utils.rabbitqAdapter import RabbitMQMessageBroker


if __name__ == '__main__':
    message_broker = RabbitMQMessageBroker('localhost', 5672, 'guest', 'guest', routing_key='extractor')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    try:
        message_broker.connect()
        logging.info('Connected to message broker')

        # Send all compressed files in a directory to the extractor topic
        directory_path = './CompressedFiles'
        compressed_files = glob.glob(os.path.join(directory_path, '*.*'))
        for compressed_file in compressed_files:
            message_broker.publish(compressed_file, 'extractor')
            logging.info(f"Published file path: {compressed_file} to extractor topic")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        message_broker.disconnect()
        logging.info('Disconnected from message broker')
