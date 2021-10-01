from collections import Set


class WbField:
    def __init__(self, lzy_proxy, deps: Set[str]):
        super(WbField, self).__init__()
        self._proxy = lzy_proxy
        self._deps = set(deps) if deps is not None else None

    @property
    def val(self):
        return self._proxy

    @property
    def deps(self):
        return set(self._deps)

    @deps.setter
    def deps(self, value):
        self._deps = set(value)
