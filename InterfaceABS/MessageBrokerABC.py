from abc import ABC, abstractmethod


class MessageBroker(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def publish(self, message, topic):
        pass

    @abstractmethod
    def subscribe(self, topic, callback):
        pass
