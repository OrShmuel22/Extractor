import gzip
import logging
from InterfaceABS.file_extractor_abs import FileExtractor

class GzipExtractor(FileExtractor):
    @property
    def file_extension(self):
        return 'gz'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with gzip.open(compressed_file_path, 'rb') as gz_file:
            with open(path_to_uncompressed_file, 'wb') as uncompressed_file:
                uncompressed_file.write(gz_file.read())

    def extract_gzip(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a gzip archive using Python's gzip library

        Args:
            compressed_file_path: The path to the compressed gzip archive
            path_to_uncompressed_file: The path to the uncompressed file

        Returns:
            True if the file is password-protected, False otherwise.
        """
        password_protected = False

        try:
            with gzip.open(compressed_file_path, 'rb') as gz_file:
                # Reading the file will check if it's password-protected
                gz_file.read()

            self.extract_file(compressed_file_path, path_to_uncompressed_file)

        except OSError as e:
            logging.error(f"Error extracting gzip file {compressed_file_path}: {e}")
            raise

        return password_protected
