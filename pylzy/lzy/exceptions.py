class LzyError(Exception):
    pass


class BadProvisioning(LzyError):
    pass


class BadClientVersion(LzyError):
    pass


class BadPypiIndex(LzyError):
    pass
