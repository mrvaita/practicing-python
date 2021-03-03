"""Read a log file and calculate how many bytes of data were transferred
Log entry example:
    81.107.39.38 - ... "GET /ply/ HTTP/1.1" 200 7587

The number of bytes is in the last column and it can be either a number
(eg 7587) or a missing value (eg -).
"""
import gzip, bz2
import re
from config import logger
from pathlib import Path


def gen_open(paths):
    for path in paths:
        if path.suffix == ".gz":
            yield gzip.open(path, "rt")
        elif path.suffix == ".bz2":
            yield bz2.open(path, "rt")
        else:
            yield open(path, "rt")


def gen_concat(sources):
    for source in sources:
        for item in source:
            yield item


def gen_grep(pattern, lines):
    pattern_compiled = re.compile(pattern)
    groups = (pattern_compiled.match(line) for line in lines)
    tuples = (g.groups() for g in groups if g)

    return tuples


def field_map(dict_logs, name, func):
    for dict_log in dict_logs:
        dict_log[name] = func(dict_log[name])
        yield dict_log


def calc_bytes(lines):
    bytecolumn = (line.rsplit(None, 1)[1] for line in lines)
    byte_sent = (int(x) for x in bytecolumn if x != "-")

    return sum(byte_sent)


def main():
    pattern = r"(\S+) (\S+) (\S+) \[(.*?)\] \"(\S+) (\S+) (\S+)\" (\S+) (\S+)"
    col_names = (
        "host",
        "referrer",
        "user",
        "datetime",
        "method",
        "request",
        "proto",
        "status",
        "bytes",
    )

    log_names = Path("www/").rglob("access-log*")
    log_files = gen_open(log_names)
    log_lines = gen_concat(log_files)
    log_groups = gen_grep(pattern, log_lines)
    log = (dict(zip(col_names, t)) for t in log_groups)
    log = field_map(log, "status", int)
    log = field_map(log, "bytes", lambda s: int(s) if s != "-" else 0)
    logger.info(next(log))
    #byte_sent = calc_bytes(log_lines)
    #logger.info(f"Total bytes = {byte_sent}")


if __name__ == "__main__":
    main()
