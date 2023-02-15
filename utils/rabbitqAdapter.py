from InterfaceABS.MessageBrokerABC import MessageBroker
import pika


class RabbitMQMessageBroker(MessageBroker):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, '/', credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def subscribe(self, topic, callback):
        self.channel.queue_declare(queue=topic)
        self.channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def publish(self, message, topic):
        self.channel.queue_declare(queue=topic)
        self.channel.basic_publish(exchange='', routing_key=topic, body=message)
