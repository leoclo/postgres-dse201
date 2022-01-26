import json
import sys


def get_settings():
    with open(sys.argv[1], "r") as openfile:
        return json.load(openfile)


def pretty_print(d):
    print(f'=======\n{json.dumps(d, sort_keys=True, indent=4)}\n======')