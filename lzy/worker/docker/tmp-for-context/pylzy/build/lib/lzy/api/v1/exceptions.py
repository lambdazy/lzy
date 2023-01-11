class LzyExecutionException(Exception):
    def __init__(self, message, *args):
        message += (
            "\nIf you are going to ask for help of cloud support,"
            " please send the following trace files: /tmp/lzy-log/"
        )
        super().__init__(message, *args)
