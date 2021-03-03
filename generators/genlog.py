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


def lines_from_dir(filepath, dirname):
    names = Path(dirname).rglob(filepath)
    files = gen_open(names)
    lines = gen_concat(files)

    return lines
    

def apache_log(lines):

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

    groups = gen_grep(pattern, lines)
    log = (dict(zip(col_names, t)) for t in groups)
    log = field_map(log, "status", int)
    log = field_map(log, "bytes", lambda s: int(s) if s != "-" else 0)

    return log



def main():

    dirname = "www/"
    filepath = "access-log*"

    log_lines = lines_from_dir(filepath, dirname)
    log = list(apache_log(log_lines))

    # Find the set of all documents that 404
    stat404 = {r["request"] for r in log if r["status"] == 404}

    # Find all request that transfer over a megabyte
    large = [r for r in log if r["bytes"] > 1000000]

    # Find the largest data transfer
    logger.info("%d %s" % max((r["bytes"], r["request"]) for r in log))

    # collect all unique host IP addresses
    hosts = {r["host"] for r in log}

    # Find the number of download of a file
    file_download = sum(1 for r in log if r["request"] == "/ply/ply-2.3.tar.gz")

    # Calculate the total number of bytes transferred
    byte_sent = sum(r["bytes"] for r in log)
    logger.info(f"Total bytes = {byte_sent}")


if __name__ == "__main__":
    main()
