import pika
import os


class BrokerAdapter:
    def __init__(self, broker_host:str, broker_port: int, pipeline_mode:bool=False):
        self.host = broker_host
        self.port = broker_port
        self.pipeline_mode = pipeline_mode
        self.init = False

    def init_adapter(self):
        if not self.pipeline_mode:
            return # don't use broker in not pipeline mode
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()
        self.queue_name = "scrapper_queue"
        self.channel.queue_declare(queue=self.queue_name)
        self.init = True

    def push_message(self, message: str):
        if not self.pipeline_mode:
            return
        if not self.init:
            raise Exception('Broker Adapter is not initialized')
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        if not self.pipeline_mode:
            return
        if not self.init:
            raise Exception('Broker Adapter is not initialized')        
        self.connection.close()