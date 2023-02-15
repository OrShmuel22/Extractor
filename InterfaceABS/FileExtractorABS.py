from abc import ABC, abstractmethod


class FileExtractor(ABC):
    @abstractmethod
    def extract_file(self, filename, path_to_extract):
        pass
