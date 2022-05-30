#!/usr/bin/env python
import argparse
import signal
import sys

from lzy.api.v1.servant.terminal_server import TerminalServer, TerminalConfig


def create_signal_handler(terminal: TerminalServer):
    def handler(*_):
        terminal.stop()
        print("Stopped terminal")
        sys.exit(0)

    return handler


def console_main():
    parser = argparse.ArgumentParser(description='lzy-terminal entrypoint')

    parser.add_argument("-s", "--server", dest="url", default="https://api.lzy.ai:8899",
                        help="Server url.\nOptional: https://api.lzy.ai:8899 is used"
                             "as default if key is not given.",
                        type=str)
    parser.add_argument("-p", "--port", dest="port", default="9999",
                        help="Terminal port. Optional: 9999 is used as default "
                             "value.")
    parser.add_argument("-q", "--fs-port", dest="fsport", default="9998",
                        help="Terminal FS port. Optional: 9998 is used as default "
                             "value.")
    parser.add_argument("-k", "--private-key-path", dest="keypath",
                        default=None,
                        help="Path to private rsa key.\n"
                             "Optional: I~/.ssh/id_rsa is used as default path"
                             " if key is not given.",
                        type=str)
    parser.add_argument("-m", "--mount", default="", dest="mountpath",
                        type=str,
                        help="Path to mounted lzy fs.\n"
                             "Optional: $LZY_MOUNT environment variable is used"
                             " as default value if key is not given. If $LZY_MOUNT is not given, /tmp/lzy is used.")
    parser.add_argument("-u", "--user", default=None, dest="user",
                        type=str, help="User name. \n"
                                       "Optional: $USER environment variable is used"
                                       " as default value if key is not given.")
    parser.add_argument("-d", "--debug-port", default="5006", dest="debug",
                        type=str, help="Port to attach java debugger.")

    args = parser.parse_args()
    config = TerminalConfig(
        server_url=args.url,
        port=args.port,
        fs_port=args.fsport,
        private_key_path=args.keypath,
        user=args.user,
        debug_port=args.debug,
        lzy_mount=args.mountpath
    )
    terminal_server = TerminalServer(config)
    print(terminal_server.jar_path)

    signal.signal(signal.SIGINT, create_signal_handler(terminal_server))
    terminal_server.start()
    print("Started terminal")
    signal.pause()
    sys.exit(0)
