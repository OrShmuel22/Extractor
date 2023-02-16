import logging
import tarfile
from InterfaceABS.file_extractor_abs import FileExtractor


class TarExtractor(FileExtractor):
    @property
    def file_extension(self):
        return 'tar'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with tarfile.open(compressed_file_path, "r:*") as tar_ref:
            tar_ref.extractall(path_to_uncompressed_file)

    def extract_tar(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a tar archive using tarfile library

        Args:
            compressed_file_path: The path to the compressed tar archive
            path_to_uncompressed_file: The path to the uncompressed file

        Returns:
            True if the file is password-protected, False otherwise.
        """

        password_protected = False

        try:
            with tarfile.open(compressed_file_path, 'r:*') as tar_file:
                for file_name in tar_file.getnames():
                    tar_file.extract(file_name, path_to_uncompressed_file)

                password_protected = False

        except tarfile.TarError as e:
            logging.error(f"Error extracting tar file {compressed_file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error extracting tar file {compressed_file_path}: {e}")
            raise

        return password_protected
