import pika
import argparse
import sys
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker_host', type=str, required=False, default='localhost',
                        help='hostname of message broker')
    parser.add_argument('--broker_port', type=int, required=False, default=5672,
                        help='port where message broker is running')
    parser.add_argument('--queue_name', type=str, required=True,
                        help='queue of corresponding producer channel')
    args = parser.parse_args()
    queue_name = args.queue_name
    host, port = args.broker_host, args.broker_port
    totally_consumed = 0
    totally_errors = 0
    result = []
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)

        def consume_callback(ch, method, properties, body):
            nonlocal totally_consumed, totally_errors, result
            try:
                msg = body.decode()
                print(f'recieved: {msg}')
                ch.basic_ack(delivery_tag=method.delivery_tag)
                result.append(json.loads(msg))
                totally_consumed += 1
            except Exception as e:
                totally_errors += 1
                print(f'exception {str(e)}; for message - {msg}')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        channel.basic_consume(queue=queue_name,
                              on_message_callback=consume_callback,
                              auto_ack=False)
        channel.start_consuming()
    except (KeyboardInterrupt, InterruptedError):
        json_string = json.dumps(result)
        with open('consumed.json', 'w', encoding='utf-8') as f:
            f.write(json_string)
        print(f'key interrupted, close the channel', file=sys.stderr)
        print('STAT:')
        print(f'totally consumed={totally_consumed}; totally errors={totally_errors}')
        channel.close()

if __name__ == '__main__':
    main()