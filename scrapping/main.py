import argparse
import asyncio
import os
from urls_scrapper import UrlExtractor
import sys
from typing import List
from settings.settings import crawler_settings


def validate_args(args):
    if args.depth < 1:
        raise Exception(f'invalid depth arg={args.d}, should be a positive value')
    if args.max_urls < 1:
        raise Exception(f'invalid max_urls arg={args.max_urls}, should be a positive value')
    if args.exclude_dirs is not None:
        try:
            for dir in args.exclude_dirs:
                os.listdir(dir)
        except Exception:
            raise Exception(f'invalid exclude directory={args.exclude_dir}')


def get_excluded_files(dirs: List[str]):
    res=[]

    for dir in dirs:
        files = os.listdir(dir)
        res = res + files
    return res


def parse_bool_str(arg:str):
    try:
        return {'true': True, 'false': False}[arg.lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'invalid bool literal: {arg}')


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base_url', type=str, default=crawler_settings.launch.base_url,
                        help='base url to start extracting content from')
    parser.add_argument('--max_urls', type=int, default=crawler_settings.launch.max_urls,
                        help='maximum count of urls to extract content from')
    parser.add_argument('--depth', type=int, default=crawler_settings.launch.depth,
                        help="maximum depth of passage through child url's")
    parser.add_argument('--output', type=str, default=crawler_settings.launch.output,
                        help='directory to store the generated documents')
    parser.add_argument('--log', type=parse_bool_str, default=crawler_settings.launch.log,
                        help='enable logging or not')
    parser.add_argument('--exclude_dirs',
                        nargs="*",
                        default=crawler_settings.exclude_dirs,
                        help="directories with files which was already been processed, "
                             "so urls corresponding to them are not been parsed")
    parser.add_argument('--ignored_domens',
                        nargs="*",
                        default=crawler_settings.ignored_domens,
                        help='urls with containing this domens will be ignored, \
                        additionally to default ignored domens')
    parser.add_argument('--required_domens',
                        nargs="*",
                        default=crawler_settings.required_domens,
                        help="urls without containing any of this domens will be ignored.")
    parser.add_argument('--use_pipeline', type=parse_bool_str, default=crawler_settings.pipeline_settings.use_pipeline,
                        help='weather to use pipeline mode with message broker or not')
    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    ignored_domens = crawler_settings.ignored_domens
    required_domens = args.required_domens
    exclude_files = get_excluded_files(args.exclude_dirs)
    urls_extractor = UrlExtractor(settings=crawler_settings,
                                  max_depth=args.depth,
                                  ignored_domens=list(ignored_domens),
                                  required_domens=required_domens,
                                  max_urls=args.max_urls,
                                  exclude_files=exclude_files,
                                  save_dir=args.output,
                                  use_pipeline=args.use_pipeline
                                  )
    await urls_extractor.extract(args.base_url, log=args.log)
    urls_extractor.save_meta_dict()   # save dict with <file_name: url> pairs


if __name__ == '__main__':
    asyncio.run(main())
    