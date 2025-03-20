from transformers import AutoTokenizer
from vllm import LLM, SamplingParams
from typing import List
import re
from tqdm import tqdm
import argparse
import sys
import os
import magic
import json
from settings.settings import formatter_settings
from broker import BrokerAdapter


MAX_TOKENS = 8192


def split_on_chunks(input_file:str, chunk_size: int):
    st_idx = 0
    end_idx = st_idx + chunk_size
    chunks = []

    with open(input_file, 'r', encoding='utf-8') as f:
        data = str(f.read())

    while st_idx < len(data):
        chunk = data[st_idx: end_idx]
        if end_idx < len(data) - 1 and re.search(r'\s$', chunk) is None:
            last_space = re.search(r'\s\S*$', chunk)
            if last_space is None:
                raise Exception('bad text: no spaces detected')
            chunk_end_idx = last_space.span()[0]
            chunk = chunk[:chunk_end_idx]
            end_idx = st_idx + chunk_end_idx
        chunks.append(chunk)
        st_idx = end_idx
        end_idx = min(st_idx + chunk_size, len(data))

    return chunks


def infer_chat(model, tokenizer, chat_template: List[dict], user_query: str, max_tokens: int = MAX_TOKENS):
    sampling_params = SamplingParams(temperature=0.7, top_p=0.8, repetition_penalty=1.05, max_tokens=max_tokens)
    messages = chat_template + [{'role': 'user', 'content': user_query}]

    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
    )
    outputs = model.generate([text], sampling_params)
    assert len(outputs) == 1

    return outputs[0].outputs[0].text


def refactor_doc(file_path: str, few_shot_prompt: List[dict], output: str, model, tokenizer, chunk_size: int):
    def add_chunk(result, chunk):
        if re.search(r'\s$', chunk) is None:
            return result + " " + chunk
        return result + chunk

    # chunk size is equal to max tokens
    print(f'file path={file_path}')
    data_chunks = split_on_chunks(file_path, chunk_size)
    result = ""
    pbar = tqdm(len(data_chunks))

    for chunk in data_chunks:
        filtered_chunk = infer_chat(model, tokenizer, few_shot_prompt, 'refactor this text: ' + chunk, max_tokens=chunk_size)
        result = add_chunk(result, filtered_chunk)
        pbar.update(1)

    with open(output, 'w', encoding='utf-8') as o:
        o.write(result)


def load_model(model_name: str):
    llm = LLM(model=model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    return llm, tokenizer


def validate_args(args):
    if args.chunk_size < 100:
        raise Exception(f'invalid chunk size={args.chunk_size}, should be 100 at least')
    if args.file_path.strip() != "" and not os.path.isfile(args.file_path):  # file is specified but doesn't exist
        raise Exception(f"file - {args.file_path} doesn't exists")
    if args.prompt_file.strip() == "":
        raise Exception(f'empty system prompt')
    if not os.path.isdir(args.dir_path) and args.file_path.strip() == "":
        raise Exception(f"input dir - {args.dir_path} doesn't exists and no input file is specified")
    if not os.path.isdir(args.output):
        os.mkdir(args.output)


def read_json(file_path:str):
    if not os.path.isfile(file_path):
        raise Exception(f"can't read json: no such file - {file_path}")
    with open(file_path, encoding='utf-8') as f:
        data = f.read()
    return json.loads(data)


def is_text(file_path: str):
    try:
        return magic.from_file(file_path, mime=True).split('/')[0] == 'text'
    except Exception:
        # issue only on cyrillic file names on windows
        return file_path.split(".")[-1] in ['txt', 'md']


def remove_markdown_artifacts(data: str) -> str:
    data = re.sub(r"#+[ \t]*([^\n\r]+)[\r\n]+", r"\1\n", data)   # remove markdown header artifact and redundant new lines
    data = re.sub(r"\*+([а-яА-Я0-9\. \t]+[:\?\.;]?)\*+", r"\1", data)
    data = re.sub(r"\*+([^*]+:?)\*+", r"\1", data)
    data = re.sub(r"_+([а-яА-Я0-9\. \t]+[:\?\.;]?)_+", r"\1", data)
    data = re.sub(r"(?<=[\n\r])[-_*]{3,}", "", data)
    data = re.sub(r"(\r?\n){3,}", "\n\n", data)

    return data


def parse_bool_str(arg:str):
    try:
        return {'true': True, 'false': False}[arg.lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'invalid bool literal: {arg}')
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', type=str, required=False, default=formatter_settings.model_path,
                        help='model path on disk')
    parser.add_argument('--file_path', type=str, required=False, default=formatter_settings.file_path,
                        help='file to refactor, if not specified, --dir_path will be used')
    parser.add_argument('--output', type=str, required=False, default=formatter_settings.output,
                        help='path to generation result directory')
    parser.add_argument('--dir_path', type=str, required=False, default=formatter_settings.dir_path,
                        help='directory to get files on refactoring from. this argument is ignored if '
                             '--file_path is specified')
    parser.add_argument('--chunk_size', type=int, required=False, default=formatter_settings.chunk_size,
                        help="chunk size of text splitting for one model inference iteration")
    parser.add_argument('--prompt_file', type=str, required=False, default=formatter_settings.prompt_file,
                        help='json file with few-shot prompt')
    parser.add_argument('--use_pipeline', type=parse_bool_str, default=formatter_settings.pipeline_settings.use_pipeline,
                        help='weather to use pipeline mode with message broker or not')
    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    model_path = args.model_path
    chunk_size = args.chunk_size
    output = args.output
    file_path = args.file_path
    dir_path = args.dir_path
    prompt_path = args.prompt_file
    few_shot_prompt = read_json(prompt_path)
    use_pipeline=args.use_pipeline

    if not use_pipeline:
        if file_path != "":
            name, ext = os.path.splitext(os.path.basename(file_path))
            if not is_text(file_path):
                print(f"WARNING: {file_path} - is not a text file, so can't be filtered")
                return
            model, tokenizer = load_model(model_path)
            refactor_doc(file_path, few_shot_prompt, os.path.join(output, name + ext),
                        model, tokenizer, chunk_size)
        else:
            files = os.listdir(dir_path)
            # select only text files:
            files = [f for f in [os.path.join(dir_path, _) for _ in files] if is_text(f)]

            if len(files) == 0:
                print(f'WARNING: no text files exists here - {dir_path}, nothing to filter')
                return
            model, tokenizer = load_model(model_path)

            for file in files:
                name, ext = os.path.splitext(os.path.basename(file))
                refactor_doc(file, few_shot_prompt, os.path.join(output, name + ext),
                            model, tokenizer, chunk_size)
                print(f'file {name + ext} is refactored', flush=True)
    else:
        adapter = BrokerAdapter(formatter_settings.pipeline_settings.broker_host,
                                formatter_settings.pipeline_settings.broker_port,
                                use_pipeline)
        adapter.init_adapter()
        model, tokenizer = load_model(model_path)                              
        print('Waiting for incoming messages...')
        def infer_callback(file_path: str):
            nonlocal adapter
            file_name = os.path.basename(file_path)
            refactor_doc(file_path, few_shot_prompt, os.path.join(output, file_name), model, tokenizer, chunk_size)
            full_path = os.path.abspath(os.path.join(output, file_name))
            adapter.push_message(full_path)   # file is processed send it to chunker
        adapter.consume_messages(infer_callback)


if __name__ == "__main__":
    main()
