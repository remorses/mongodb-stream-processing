import json
from datetime import datetime
import inspect
import json
from .logger import logger


def pretty(x): return print(json.dumps(x, indent=4, default=str))


def prettify(x): return json.dumps(x, indent=4, default=str)





def function_name(): return inspect.stack()[1][3]


def dumps(x, indent=0):
    string = json.dumps(x, indent=4, default=str)
    lines = [indent*' ' + line for line in string.split('\n')]
    return '\n'.join(lines)


def log(x): return print(json.dumps(x, indent=4, default=str))


def dicts_set(a, b, unique_props):
    """
    b has precedence
    """
    def unique_identifier(d): return tuple([d.get(s) for s in unique_props])
    d_a = {unique_identifier(d): d for d in a}
    d_b = {unique_identifier(d): d for d in b}
    return list({**d_a, **d_b}.values())


def round_time_to(timestamp, seconds=60*30):
    t = int(timestamp)
    return int(t - (t % seconds))


if __name__ == '__main__':

    for i in range(20):
        t = round_time_to(datetime.utcnow().timestamp() - 60*23*i, 60*60)
        print(datetime.fromtimestamp(t))


