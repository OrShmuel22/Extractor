import logging
from InterfaceABS.MessageBrokerABC import MessageBroker

import pika


class RabbitMQMessageBroker(MessageBroker):
    def __init__(self, host, port, username, password, routing_key=None, exchange_type='direct'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        self.logger = logging.getLogger(__name__)
        self.routing_key = routing_key
        self.exchange_type = exchange_type

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(self.host, self.port, '/', credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Automatically create exchange and bind to it
            self.channel.exchange_declare(exchange=self.routing_key, exchange_type=self.exchange_type)
            self.channel.queue_declare(queue=self.routing_key, durable=True)
            self.channel.queue_bind(exchange=self.routing_key, queue=self.routing_key, routing_key=self.routing_key)

        except Exception as e:
            self.logger.debug(f"Error connecting to RabbitMQ: {e}")
            raise

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            self.logger.debug(f"Error disconnecting from RabbitMQ: {e}")
            raise

    def subscribe(self, topic, callback):
        try:
            self.channel.basic_consume(queue=topic, on_message_callback=lambda ch, method, properties, body: self._on_message(ch, method, properties, body, callback), auto_ack=False)
            self.channel.start_consuming()
        except Exception as e:
            self.logger.debug(f"Error subscribing to RabbitMQ topic {topic}: {e}")
            raise

    def _on_message(self, channel, method, properties, body, callback):
        try:
            # Call the user-provided callback with the message body
            callback(body)
            # Acknowledge the message if it was processed successfully
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            # Reject the message (without requeueing it) if it was not processed successfully
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            self.logger.debug(f"Error processing message from RabbitMQ: {e}")

    def publish(self, message, topic):
        try:
            self.channel.basic_publish(exchange=self.routing_key, routing_key=topic, body=message)
        except Exception as e:
            self.logger.debug(f"Error publishing message to RabbitMQ with routing key {topic}: {e}")
            raise
