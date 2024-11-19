import time

from pika import PlainCredentials, ConnectionParameters, BlockingConnection, exceptions, BasicProperties


class RabbitMQConnection:
    _instance = None

    def __new__(cls, host, port, virtual_host, username, password):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host, port, virtual_host, username, password):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        wait_time = 5
        while True:
            try:
                credentials = PlainCredentials(self.username, self.password)
                parameters = ConnectionParameters(host=self.host, port=self.port,
                                                  virtual_host= self.virtual_host, credentials=credentials)
                self.connection = BlockingConnection(parameters)
                print("Connected to RabbitMQ")
                return
            except exceptions.AMQPConnectionError as e:
                print('Failed to connect to RabbitMQ:', e)
                print(f'Retrying in {wait_time} seconds...')
                time.sleep(wait_time)

    def is_connected(self):
        return self.connection is not None and self.connection.is_open

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None
            print('Closed RabbitMQ connection')

    def get_channel(self):
        if self.is_connected():
            return self.connection.channel()
        return None



class RabbitMQProducer:
    def __init__(self, connection):
        self.connection = connection
        self.channel = None

    def publish(self, queue_name, message):
        if self.channel is None:
            self.channel = self.connection.get_channel()
        if self.channel is not None:
            try:
                self.channel.queue_declare(queue=queue_name, durable=True)
                self.channel.basic_publish(exchange='',
                                           routing_key=queue_name,
                                           body=message,
                                           properties=BasicProperties(
                                               delivery_mode=2,  # make message persistent
                                           ))
            except exceptions.ConnectionClosedByBroker:
                print("Connection closed by broker. Failed to publish the message")
        else:
            print('Failed to obtain a channel for publishing the message')