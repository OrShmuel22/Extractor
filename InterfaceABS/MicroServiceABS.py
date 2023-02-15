from abc import ABC, abstractmethod


class Microservice(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def execute(self, message):
        pass
