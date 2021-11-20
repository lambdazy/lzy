from typing import Optional, Set


class WbField:
    def __init__(self, lzy_proxy, deps: Optional[Set[str]]):
        super(WbField, self).__init__()
        self._proxy = lzy_proxy
        self._deps = deps

    @property
    def val(self):
        return self._proxy

    @property
    def deps(self) -> Optional[Set[str]]:
        return self._deps

    @deps.setter
    def deps(self, value: Optional[Set[str]]):
        self._deps = value
