import logging
import py7zr
from InterfaceABS.file_extractor_abs import FileExtractor


class SevenZipExtractor(FileExtractor):
    @property
    def file_extension(self):
        return '7z'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with py7zr.SevenZipFile(compressed_file_path, mode='r') as archive:
            archive.extractall(path_to_uncompressed_file)

    def extract_7z(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a 7z archive using the py7zr library.

        Args:
            compressed_file_path: The path to the compressed 7z archive.
            path_to_uncompressed_file: The path to the uncompressed file.

        Returns:
            True if the file is password-protected, False otherwise.
        """

        password_protected = False

        try:
            with py7zr.SevenZipFile(compressed_file_path, mode='r') as archive:
                archive.extractall(path_to_uncompressed_file)

                if archive.psw is not None:
                    password_protected = True

        except py7zr.Bad7zFile as e:
            logging.error(f"Error extracting 7z file {compressed_file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error extracting 7z file {compressed_file_path}: {e}")
            raise

        return password_protected
