from abc import ABC, abstractmethod


class Microservice(ABC):
    @abstractmethod
    def execute(self, message):
        pass
