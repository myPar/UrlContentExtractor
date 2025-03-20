from smart_chunker.chunker import SmartChunker
from settings.settings import chunker_settings
import argparse
import sys
import os
from broker import BrokerAdapter
import magic
import torch


def validate_args(args):
    # validate output directory
    if not args.output:
        raise ValueError(f"empty directory name for 'output' arg")
    if not os.path.isdir(args.output):
        try:
            os.mkdir(args.output)
        except Exception:
            raise ValueError(f"can't create directory with name {args.output}")

    # validate language
    valid_languages = ['ru', 'en']
    if args.lang and args.lang not in valid_languages:
        raise ValueError(f"Invalid language '{args.lang}'. Available languages: {valid_languages}.")
    # validate chunk_size
    if args.chunk_size <= 0:
        raise ValueError(f"Chunk size must be a positive integer. Got '{args.chunk_size}'.")
    # validate delimiter
    if not args.delimiter:
        raise ValueError("Delimiter cannot be empty.")

    # skip file_path and dir_path validation if pipeline mode is enabled (files for processing are getting from broker)
    if not args.use_pipeline:
        if args.file_path:
            if not os.path.exists(args.file_path):
                raise ValueError(f"File path '{args.file_path}' does not exist.")
            if not is_text(args.file_path):
                raise ValueError(f"File '{args.file_path}' is not a text file.")

        if not args.file_path and args.dir_path:
            if not os.path.isdir(args.dir_path):
                raise ValueError(f"Directory path '{args.dir_path}' does not exist or is not a directory.")


def is_text(file_path: str):
    try:
        return magic.from_file(file_path, mime=True).split('/')[0] == 'text'
    except Exception:
        # issue only on cyrillic file names on windows
        return file_path.split(".")[-1] in ['txt', 'md']


def chunk(file: str, output: str, chunker: SmartChunker, delimiter:str='\n'*4):
    file_name = os.path.basename(file)

    with open(file, 'r', encoding='utf-8') as f:
        data = f.read()
    chunks = chunker.split_into_chunks(data)
    result_data = delimiter.join(chunks).strip()

    with open(os.path.join(output, file_name), 'w', encoding='utf-8') as f:
        f.write(result_data)


def parse_bool_str(arg:str):
    try:
        return {'true': True, 'false': False}[arg.lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'invalid bool literal: {arg}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', type=str, required=False, default=chunker_settings.model_path,
                        help='path to the model for processing')
    parser.add_argument('--file_path', type=str, required=False, default=chunker_settings.file_path,
                        help='path to a specific file to process. overrides --dir_path if specified')
    parser.add_argument('--output', type=str, required=False, default=chunker_settings.output,
                        help='path to save the output file')
    parser.add_argument('--dir_path', type=str, required=False, default=chunker_settings.dir_path,
                        help='directory to get files for refactoring from. this argument is ignored if '
                             '--file_path is specified')
    parser.add_argument('--lang', type=str, required=False, default=chunker_settings.lang,
                        help="language of the text to process (available: 'ru', 'en')")
    parser.add_argument('--chunk_size', type=int, required=False, default=chunker_settings.chunk_size,
                        help='size of chunks to process')
    parser.add_argument('--use_pipeline', type=parse_bool_str, required=False, default=chunker_settings.pipeline_settings.use_pipeline,
                        help='weather use pipeline mode with message broker or not') 
    parser.add_argument('--delimiter', type=str, required=False, default=chunker_settings.delimiter,
                        help="delimiter between splitted chunks (default: '\\n\\n\\n\\n')")
    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    output = args.output
    file_path = args.file_path
    dir_path = args.dir_path
    delimiter = args.delimiter

    chunker = SmartChunker(
                language=args.lang,
                reranker_name=args.model_path,
                newline_as_separator=False,
                device='cuda:0' if torch.cuda.is_available() else 'cpu',
                max_chunk_length=args.chunk_size,
                minibatch_size=8,
                verbose=True
              )
    if args.use_pipeline:
        # use broker:
        adapter = BrokerAdapter(chunker_settings.pipeline_settings.broker_host, 
                                chunker_settings.pipeline_settings.broker_port, 
                                args.use_pipeline)
        adapter.init_adapter()
        print('Waiting incoming messages...')

        def infer_callback(file_path: str):
            file_name = os.path.basename(file_path)

            with open(file_path, 'r', encoding='utf-8') as f:
                data = f.read()
            chunks = chunker.split_into_chunks(data)
            result_text = delimiter.join(chunks)

            with open(os.path.join(output, file_name), 'w', encoding='utf-8') as f:
                f.write(result_text.strip())
        adapter.consume_messages(infer_callback)
    else:
        # run as simple python script
        if file_path != "":
            if not is_text(file_path):
                print(f"WARNING: {file_path} - is not a text file, so can't be chunked")
                return
            # chunking:
            chunk(file_path, output, chunker, delimiter=delimiter)
        else:
            files = os.listdir(dir_path)
            # select only text files:
            files = [f for f in [os.path.join(dir_path, _) for _ in files] if is_text(f)]

            if len(files) == 0:
                print(f'WARNING: no text files exists here - {dir_path}, nothing to chunk')
                return

            for file in files:
                chunk(file, output, chunker, delimiter=delimiter)
                # chunking
                print(f'file {os.path.basename(file)} is chunked', flush=True)


if __name__ == '__main__':
    main()