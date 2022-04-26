import argparse
import json
import os
import logging
import logging.config

def get_logger():
    this_dir, _ = os.path.split(__file__)
    path = os.path.join(this_dir, 'logging.conf')
    logging.config.fileConfig(path)
    return logging.getLogger()

def load_json(path):
    with open(path) as fil:
        return json.load(fil)

def parse_args():
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -r,--raw      Raw event source config
    -p,--pending  Pending session target config
    -c,--custom   Custom cros session target config

    Returns the parsed args object from argparse. For each argument that
    point to JSON files, we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-r', '--raw',
        help='Raw event source config',
        required=True)

    parser.add_argument(
        '-p', '--pending',
        help='Pending session target config',
        required=True)

    parser.add_argument(
        '-c', '--custom',
        help='Custom cros session target config')

    args = parser.parse_args()
    if args.raw:
        args.raw = load_json(args.raw)
    if args.pending:
        args.pending = load_json(args.pending)
    if args.custom:
        args.custom = load_json(args.custom)

    return args