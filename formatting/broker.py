import pika
import sys


class BrokerAdapter:
    def __init__(self, broker_host:str, broker_port: int, pipeline_mode:bool=False):
        self.host = broker_host
        self.port = broker_port
        self.pipeline_mode = pipeline_mode
        self.init = False

        self.connection = None
        self.channel = None
        self.consume_queue_name = None
        self.produce_queue_name = None

    def init_adapter(self):
        if not self.pipeline_mode:
            return # don't use broker in not pipeline mode
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()
        self.consume_queue_name = "scrapper_queue"
        self.produce_queue_name = "formatter_queue"

        self.channel.queue_declare(queue=self.consume_queue_name)
        self.channel.queue_declare(queue=self.produce_queue_name)

        self.init = True

    def push_message(self, message: str):
        if not self.pipeline_mode:
            return
        if not self.init:
            raise Exception('Broker Adapter is not initialized')
        self.channel.basic_publish(exchange='', routing_key=self.produce_queue_name, body=message)

    def consume_messages(self, infer_callback):
        if not self.init:
            raise Exception('Broker Adapter is not initialized')        
        def consume_callback(ch, method, properties, body):
            try:
                msg = body.decode()
                infer_callback(msg)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f'exception {str(e)}; for message - {msg}')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        self.channel.basic_consume(queue=self.consume_queue_name,
                                    on_message_callback=consume_callback,
                                    auto_ack=False)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.close()
            sys.exit(1)
            

    def close(self):
        if not self.pipeline_mode:
            return
        if not self.init:
            raise Exception('Broker Adapter is not initialized')
        self.connection.close()
