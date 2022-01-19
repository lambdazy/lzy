#!/usr/bin/env python
import argparse
import signal
import sys

from lzy.servant.terminal_server import TerminalConfig, TerminalServer


def create_signal_handler(terminal: TerminalServer):
    def handler(*_):
        terminal.stop()
        print("Stopped terminal")
        sys.exit(0)

    return handler


def console_main():
    parser = argparse.ArgumentParser(description='lzy-terminal entrypoint')

    parser.add_argument("--url", dest="url", default="api.lzy.ai:8899",
                        help="Server url.\nOptional: api.lzy.ai:8899 is used"
                             "as default if key is not given.",
                        type=str)
    parser.add_argument("-k", "--private-key-path", dest="keypath",
                        default="~/.ssh/id_rsa",
                        help="Path to private rsa key.\n"
                             "Optional: I~/.ssh/id_rsa is used as default path"
                             " if key is not given.",
                        type=str)
    parser.add_argument("-m", "--mount", default="", dest="mountpath",
                        type=str,
                        help="Path to mounted lzy fs.\n"
                             "Optional: $LZY_MOUNT environment variable is used"
                             " as default value if key is not given.")
    parser.add_argument("-u", "--user", default=None, dest="user",
                        type=str, help="User name.")

    args = parser.parse_args()
    config = TerminalConfig(
        server_url=args.url,
        private_key_path=args.keypath,
        user=args.user,
        lzy_mount=args.mountpath
    )
    terminal_server = TerminalServer(config)

    signal.signal(signal.SIGINT, create_signal_handler(terminal_server))

    terminal_server.start()
    terminal_server.join()
    sys.exit(0)
