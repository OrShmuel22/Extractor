import logging
import os

import magic
from os.path import join, splitext
from InterfaceABS.micro_service_abs import Microservice
from extractors.file_extractors import TarExtractor, Bzip2Extractor, XzExtractor, GzipExtractor, ZipExtractor, \
    SevenZipExtractor, RarExtractor
from extractors.file_extractors import PasswordRequired, DamagedArchive


class ExtractorHandler(Microservice):
    def __init__(self):
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

    def execute(self, message):
        message = message.decode()
        logging.debug(f"Received file: {message}")
        file_type = self.get_file_type(message)
        logging.debug(f"File_type: {file_type}")
        if file_type in self.extractor_config:
            extractor_class = self.extractor_config[file_type]
            base_name, extension = os.path.splitext(message)
            try:
                with extractor_class() as extractor:
                    extractor.extract_file(message, join(self.path_to_uncompressed_file, base_name + extension[1:]))
                logging.info(f"Extracted file: {message}")
            except DamagedArchive as e:
                logging.warning(f"{e}: {message}")
            except PasswordRequired as e:
                logging.warning(f"{e}: {message}")
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


if __name__ == '__main__':
    compressed_files_directory = 'CompressedFiles'
    uncompressed_files_directory = 'UncompressedFiles'
    os.makedirs(uncompressed_files_directory, exist_ok=True)

    extractor_config = {
        "application/x-tar": TarExtractor,
        "application/x-bzip2": Bzip2Extractor,
        "application/x-xz": XzExtractor,
        "application/gzip": GzipExtractor,
        "application/x-gzip": GzipExtractor,
        "application/zip": ZipExtractor,
        "application/x-7z-compressed": SevenZipExtractor,
        "application/x-rar": RarExtractor
    }

    extractor_handler = ExtractorHandler()
    extractor_handler.extractor_config = extractor_config
    extractor_handler.path_to_uncompressed_file = uncompressed_files_directory

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    for file_name in os.listdir(compressed_files_directory):
        file_path = os.path.join(compressed_files_directory, file_name)
        if os.path.isfile(file_path):
            extractor_handler.execute(file_path.encode())
