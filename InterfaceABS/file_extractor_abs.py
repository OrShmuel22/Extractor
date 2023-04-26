from abc import ABC, abstractmethod

class FileExtractor(ABC):

    @abstractmethod
    def extract_file(self, compressed_file_path, path_to_uncompressed_file):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
