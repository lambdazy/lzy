from __future__ import annotations

from typing import Collection, TYPE_CHECKING

from lzy.api.v1 import Runtime
from lzy.core.workflow import LzyWorkflow
from tests.api.v1.mocks import RuntimeMock

if TYPE_CHECKING:
    from lzy.core.call import LzyCall


class TestLzyWorkflow(LzyWorkflow):
    @property
    def calls(self) -> Collection[LzyCall]:
        runtime: Runtime = self.owner.runtime
        assert isinstance(runtime, RuntimeMock), 'wf.calls may be used only with mock runtime'
        return runtime.calls

    @property
    def first_call(self) -> LzyCall:
        calls = self.calls
        assert calls
        return calls[0]
