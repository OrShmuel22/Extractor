import importlib
import logging
import magic
from InterfaceABS.micro_service_abs import Microservice
from utils.rabbitmq_adapter import RabbitMQMessageBroker
from os.path import join,splitext


class ExtractorHandler(Microservice):
    def __init__(self, message_broker):
        self.message_broker = message_broker
        # Todo: Change extractor dict to get information from mongodb about out different type of extract
        self.extractor_config = {
            "application/x-tar": "extractors.tar_extractor.TarExtractor",
            "application/x-bzip2": "extractors.bzip_extractor.Bzip2Extractor",
            "application/x-xz": "extractors.xz_extractor.XzExtractor",
            "application/gzip": "extractors.gzip_extractor.GzipExtractor",
            "application/zip": "extractors.zip_extractor.ZipExtractor",
            "application/x-7z-compressed": "extractors.7z_extractor.SevenZipExtractor",
            "application/x-rar": "extractors.rar_extractor.RarExtractor"
        }

        self.path_to_uncompressed_file = "./path_to_uncompressed_file/"
        self.logger = logging.getLogger(__name__)

    def start(self):
        self.message_broker.connect()
        logging.info('Connected to message broker')

    def stop(self):
        self.message_broker.disconnect()
        logging.info('Disconnected from message broker')

    def execute(self, message):
        message = message.decode()
        logging.debug(f"Received file: {message}")
        file_type = self.get_file_type(message)
        logging.debug(f"File_type: {file_type}")
        if file_type in self.extractor_config:
            extractor_class = self.load_extractor_class(file_type, self.extractor_config)
            extractor = extractor_class()
            base_name, extension = splitext(message)
            extractor.extract_file(message, join(self.path_to_uncompressed_file, base_name + extension[1:]))
            logging.info(f"Extracted file: {message}")

        else:
            logging.info(f"File type not extractable: {file_type}")

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

    # Todo: implement password brute force when file is protected
    def password_brute_force(self):
        pass

    @staticmethod
    def load_extractor_class(file_type, extractor_config):
        class_name = extractor_config.get(file_type)

        if class_name is not None:
            try:
                module_name, class_name = class_name.rsplit('.', 1)
                module = importlib.import_module(module_name)
                extractor_class = getattr(module, class_name)
                logging.debug(f"Loaded extractor class {class_name} for file type {file_type}")
                return extractor_class
            except (ImportError, AttributeError) as e:
                logging.exception(f"Failed to load extractor class {class_name} for file type {file_type}: {e}")
                raise
        else:
            logging.debug(f"No extractor class found for file type {file_type}")
            raise


if __name__ == '__main__':
    message_broker = RabbitMQMessageBroker('localhost', 5672, 'guest', 'guest', routing_key='extractor')
    extractor = ExtractorHandler(message_broker)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(__name__)

    try:
        extractor.start()
        extractor.message_broker.subscribe(callback=extractor.execute, topic='extractor')
    except KeyboardInterrupt:
        extractor.stop()
