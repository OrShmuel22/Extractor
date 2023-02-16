import logging
import rarfile
from InterfaceABS.file_extractor_abs import FileExtractor

class RarExtractor(FileExtractor):
    @property
    def file_extension(self):
        return 'rar'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with rarfile.RarFile(compressed_file_path, "r") as rar_ref:
            rar_ref.extractall(path_to_uncompressed_file)

    def extract_rar(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a rar archive using rarfile library

        Args:
            compressed_file_path: The path to the compressed rar archive
            path_to_uncompressed_file: The path to the uncompressed file

        Returns:
            True if the file is password-protected, False otherwise.
        """

        password_protected = False

        try:
            with rarfile.RarFile(compressed_file_path, 'r') as rar_file:
                for file_name in rar_file.namelist():
                    rar_file.extract(file_name, path_to_uncompressed_file)

                if rar_file.needs_password():
                    password_protected = True

        except rarfile.BadRarFile as e:
            logging.error(f"Error extracting rar file {compressed_file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error extracting rar file {compressed_file_path}: {e}")
            raise

        return password_protected
