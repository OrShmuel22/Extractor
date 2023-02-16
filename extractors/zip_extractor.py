import logging
import zipfile
from InterfaceABS.file_extractor_abs import FileExtractor


class ZipExtractor(FileExtractor):
    @property
    def file_extension(self):
        return 'zip'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with zipfile.ZipFile(compressed_file_path, "r") as zip_ref:
            zip_ref.extractall(path_to_uncompressed_file)

    def extract_zip_zipfile(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a zip archive using Python's zipfile library

        Args:
            compressed_file_path: The path to the compressed zip archive
            path_to_uncompressed_file: The path to the uncompressed file

        Returns:
            True if the file is password-protected, False otherwise.
        """

        password_protected = False

        try:
            with zipfile.ZipFile(compressed_file_path, 'r') as zip_file:
                for file_name in zip_file.namelist():
                    zip_file.extract(file_name, path_to_uncompressed_file)

                if zip_file.getinfo(zip_file.namelist()[0]).flag_bits == 0x1:
                    password_protected = True

        except zipfile.BadZipFile as e:
            logging.error(f"Error extracting zip file {compressed_file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error extracting zip file {compressed_file_path}: {e}")
            raise

        return password_protected
