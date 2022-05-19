from dataclasses import dataclass
from typing import List

from lzy.v2.api.servant.model.channel import Bindings
from lzy.v2.api.servant.model.zygote import Zygote


@dataclass
class TaskSpec:
    call_id: str
    operation_name: str
    zygote: Zygote
    bindings: Bindings
    dependent_calls: List[str]
