import json
import argparse
import os
import sys


def parse_text_to_prompts(text):
    """
    Parses the text into a list of prompt dictionaries, handling multi-line content.
    """
    prompts = []
    lines = text.strip().split('\n')
    current_role = None
    current_content = []

    for line in lines:
        if line.lower().startswith("system:"):
            if current_role is not None:
                prompts.append({"role": current_role.lower(), "content": "\n".join(current_content).strip()})
                current_content = []
            current_role = "system"
        elif line.lower().startswith("user:"):
            if current_role is not None:
                prompts.append({"role": current_role.lower(), "content": "\n".join(current_content).strip()})
                current_content = []
            current_role = "user"
        elif line.lower().startswith("assistant:"):
            if current_role is not None:
                prompts.append({"role": current_role.lower(), "content": "\n".join(current_content).strip()})
                current_content = []
            current_role = "assistant"
        else:
            # Append to the current role's content
            current_content.append(line.strip())

    # Add the last prompt
    if current_role and current_content:
        prompts.append({"role": current_role.lower(), "content": "\n".join(current_content).strip()})

    return prompts

def create_prompt_json(input_file, output_file):
    """
    Reads a text file, parses it, and writes the prompts to a JSON file.
    """
    with open(input_file, 'r', encoding='utf-8') as file:
        text = file.read()

    prompts = parse_text_to_prompts(text)

    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(prompts, file, indent=2)


def validate_args(args):
    if not os.path.isfile(args.input_path):
        raise Exception(f'no such file: {args.input_path}', file=sys.stderr)
    if not os.path.isdir(args.output_dir):
        try:
            os.mkdir(args.output_dir)
        except Exception as e:
            raise Exception(f"can't create output dir with name: {args.output_dir}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str, required=True,
                        help='path to input text prompt file')
    parser.add_argument('--output_dir', type=str, required=False, default=".",
                        help='output dir where json file will be placed')
    args = parser.parse_args()
    input_path = args.input_path
    output_dir = args.output_dir

    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    create_prompt_json(input_path, output_file=os.path.join(output_dir, 'prompt.json'))


if __name__ == '__main__':
    main()