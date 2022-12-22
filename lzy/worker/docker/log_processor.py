import json
import re
import sys


def main():

    prog = re.compile(r"^(\d\d\d\d-\d\d-\d\d\s+\d\d:\d\d:\d\d.\d\d\d)\s+\[(.*)\]\s+(.*)\s+(.*)\s+-\s+(.*)")

    while True:
        line = sys.stdin.readline().rstrip()
        if len(line) == 0:
            break

        message = json.loads(line)
        payload = message["payload"]

        parse = prog.match(payload)

        meta = {
            "timestamp": parse.group(1),
            "thread": parse.group(2),
            "level": parse.group(3),
            "logger": parse.group(4)
        } | message["meta"]

        message["meta"] = meta

        json.dump(message, sys.stdout)
        sys.stdout.write('\n')
        sys.stdout.flush()


if __name__ == "__main__":
    main()


