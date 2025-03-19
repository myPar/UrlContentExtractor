import argparse
import sys
import pika
import os


def validate_args(args):
    if not os.path.isdir(args.dir_path):
        raise Exception(f'directory - dir_path={args.dir_path} not exists')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker_host', type=str, required=False, default='localhost',
                        help='hostname of message broker')
    parser.add_argument('--broker_port', type=int, required=False, default=5672,
                        help='port where message broker is running')
    parser.add_argument('--queue_name', type=str, required=True,
                        help='producer queue name')
    parser.add_argument('--dir_path', type=str, required=True,
                        help='directory to extract file names from')
    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    queue_name = args.queue_name
    host, port = args.broker_host, args.broker_port
    dir_path = args.dir_path
    files = os.listdir(dir_path)
    
    totally_pushed = 0
    totally_errors = 0

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)

        for file in files:
            abs_file_path = os.path.abspath(os.path.join(dir_path, file))
            try:
                channel.basic_publish(exchange='', routing_key=queue_name, body=abs_file_path)
                totally_pushed += 1
            except Exception as e:
                totally_errors += 1
                print(f'publish error for msg - {abs_file_path}')
    except (KeyboardInterrupt, InterruptedError):
        print(f'key interrupted, close the channel', file=sys.stderr)
        channel.close()
    print('STAT:')
    print(f'totally published={totally_pushed}; totally errors={totally_errors}')       


if __name__ == '__main__':
    main()