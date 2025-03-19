import argparse


def parse_bool_str(arg:str):
    try:
        return {'true': True, 'false': False}[arg.lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'invalid bool literal: {arg}')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_pipeline', type=parse_bool_str, default=False)
    args = parser.parse_args()
    print(args.use_pipeline)

if __name__ == '__main__':
    main()