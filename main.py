import argparse
import os

from content_extractor import UrlContentExtractor
from urls_extractor import UrlExtractor
import sys
from typing import List

default_ignored_domens = {'vk.com',
                          't.me',
                          'rutube.ru',
                          'dzen.ru',
                          'youtube.com',
                          '.css',
                          'zimbra.com',
                          'youtu.be',
                          'ok.ru',
                          'apple.com',
                          'alfabank.ru'}


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base_url', type=str, required=True,
                        help='base url to start extracting content from')
    parser.add_argument('--max_urls', type=int, required=False, default=50,
                        help='maximum count of urls to extract content from')
    parser.add_argument('--depth', type=int, default=2, required=False,
                        help="maximum depth of passage through child url's")
    parser.add_argument('--reject_http', type=bool, required=False, default=False,
                        help='weather http pages are rejected to be parsed or not')
    parser.add_argument('--output', type=str, default='data/', required=False,
                        help='directory to store the generated documents')
    parser.add_argument('--log', type=bool, required=False, default=False,
                        help='enable logging or not')
    parser.add_argument('--exclude_dirs',
                        nargs="*",
                        required=False,
                        default=[],
                        help="directories with files which was already been processed, "
                             "so urls corresponding to them are not been parsed")
    parser.add_argument('--ignored_domens',
                        nargs="*",
                        required=False,
                        default=[],
                        help='urls with containing this domens will be ignored, \
                        additionally to default ignored domens')
    parser.add_argument('--required_domens',
                        nargs="*",
                        required=False,
                        default=[],
                        help="urls without containing any of this domens will be ignored.")
    try:
        args = parser.parse_args()
        validate_args(args)
    except Exception as e:
        print('Parse args exception: ' + repr(e), file=sys.stderr)
        parser.print_help()
        return
    ignored_domens = default_ignored_domens.union(set(args.ignored_domens))
    required_domens = args.required_domens
    urls_extractor = UrlExtractor(max_depth=args.depth,
                                  reject_http=args.reject_http,
                                  ignored_domens=list(ignored_domens),
                                  required_domens=required_domens,
                                  max_urls=args.max_urls
                                  )
    urls_extractor.set_max_urls(args.max_urls)
    urls = urls_extractor.extract(args.base_url, log=args.log)

    exclude_files = get_excluded_files(args.exclude_dirs)
    content_extractor = UrlContentExtractor(urls=urls, save=True, save_dir=args.output, exclude_files=exclude_files)
    content_extractor.extract_content(log=args.log)
    content_extractor.save_url_dict()   # save dict with <file_name: url> pairs


if __name__ == '__main__':
    main()