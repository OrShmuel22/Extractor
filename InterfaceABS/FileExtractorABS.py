from abc import ABC, abstractmethod


class FileExtractor(ABC):
    @abstractmethod
    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        pass
