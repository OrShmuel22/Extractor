import io
import logging
import magic
from InterfaceABS.MicroServiceABS import Microservice
from utils.rabbitqAdapter import RabbitMQMessageBroker


class ExtractorHandler(Microservice):
    def __init__(self, message_broker):
        self.message_broker = message_broker
        # Todo: get it from configuration file with different types of extractor we have
        self.extractors = {}
        self.logger = logging.getLogger(__name__)

    def start(self):
        self.message_broker.connect()
        logging.info('Connected to message broker')

    def stop(self):
        self.message_broker.disconnect()
        logging.info('Disconnected from message broker')

    def execute(self, message):
        # Add your message extraction logic here
        logging.info(f"Received file: {message}")
        file_type = self.get_file_type(message)
        logging.info(f"File_type: {file_type}")

    def _is_protected(self, file_path):
        pass

    def _is_extractable(self, file_path):
        pass

    # TODO: Move this function to "global util" it can be used for different purposes
    def get_file_type(self, file_path):
        # recommend using at least the first 2048 bytes, as less can produce incorrect identification
        try:
            file_type = magic.from_buffer(open(file_path, "rb").read(2048), mime=True)
            return file_type
        except Exception as e:
            self.logger.debug(f"Error during get file type: {e}")
            raise


if __name__ == '__main__':
    message_broker = RabbitMQMessageBroker('localhost', 5672, 'guest', 'guest', routing_key='extractor')
    extractor = ExtractorHandler(message_broker)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    try:
        extractor.start()
        extractor.message_broker.subscribe(callback=extractor.execute, topic='extractor')
    except KeyboardInterrupt:
        extractor.stop()
