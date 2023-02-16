import logging
import bz2
from InterfaceABS.file_extractor_abs import FileExtractor


class Bzip2Extractor(FileExtractor):
    @property
    def file_extension(self):
        return 'bz2'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with bz2.BZ2File(compressed_file_path, "r") as bz2_ref:
            with open(path_to_uncompressed_file, 'wb') as output:
                for line in bz2_ref:
                    output.write(line)

    def extract_bz2(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of a bzip2 archive using Python's bz2 library

        Args:
            compressed_file_path: The path to the compressed bzip2 archive
            path_to_uncompressed_file: The path to the uncompressed file
        """

        try:
            self.extract_file(compressed_file_path, path_to_uncompressed_file)
        except Exception as e:
            logging.error(f"Error extracting bzip2 file {compressed_file_path}: {e}")
            raise
