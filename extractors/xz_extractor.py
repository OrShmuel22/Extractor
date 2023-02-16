import logging
import lzma
from InterfaceABS.file_extractor_abs import FileExtractor


class XZExtractor(FileExtractor):
    @property
    def file_extension(self):
        return 'xz'

    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        with lzma.open(compressed_file_path, "rb") as xz_ref:
            with open(path_to_uncompressed_file, 'wb') as output:
                for line in xz_ref:
                    output.write(line)

    def extract_xz_lzma(self, compressed_file_path, path_to_uncompressed_file):
        """Extracts the content of an xz or lzma archive using Python's lzma library

        Args:
            compressed_file_path: The path to the compressed xz or lzma archive
            path_to_uncompressed_file: The path to the uncompressed file
        """

        try:
            self.extract_file(compressed_file_path, path_to_uncompressed_file)
        except Exception as e:
            logging.error(f"Error extracting xz or lzma file {compressed_file_path}: {e}")
            raise
