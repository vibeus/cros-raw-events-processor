import argparse
import json
import os
import re
import logging
import logging.config

def get_logger():
    this_dir, _ = os.path.split(__file__)
    path = os.path.join(this_dir, 'logging.conf')
    logging.config.fileConfig(path)
    return logging.getLogger()

def expand_env(config):
    if config is None:
        return

    assert isinstance(config, dict)

    def repl(match):
        env_key = match.group(1)
        return os.environ.get(env_key, "")

    def expand(v):
        assert not isinstance(v, dict)
        if isinstance(v, str):
            return re.sub(r"env\[(\w+)\]", repl, v)
        else:
            return v

    copy = {}
    for k, v in config.items():
        if isinstance(v, dict):
            copy[k] = expand_env(v)
        elif isinstance(v, list):
            copy[k] = [expand_env(x) if isinstance(x, dict) else expand(x) for x in v]
        else:
            copy[k] = expand(v)

    return copy

def load_json(path):
    with open(path) as fil:
        return json.load(fil)

def parse_args():
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -r,--raw            Raw event source config
    -c,--cros           Cros sessions target config
    -i,--intermediate   Intermediate storage target config

    Returns the parsed args object from argparse. For each argument that
    point to JSON files, we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-r', '--raw',
        help='Raw events source config.',
        required=True)

    parser.add_argument(
        '-c', '--cros',
        help='Cros sessions target config. If specified, should be different from pending session config.',
        required=True)

    parser.add_argument(
        '-i', '--intermediate',
        help='Intermediate storage target config.')

    parser.add_argument(
        '-s', '--state',
        help='State file.')

    parser.add_argument(
        '--debug',
        action="store_true",
        help='Debug mode.')

    parser.add_argument(
        '--drop',
        action="store_true",
        help='Drop tables.')

    args = parser.parse_args()
    if args.raw:
        args.raw = load_json(args.raw)
    if args.cros:
        args.cros = load_json(args.cros)
    if args.intermediate:
        args.intermediate = load_json(args.intermediate)
    if args.state:
        args.state = load_json(args.state)

    return args