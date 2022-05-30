from dataclasses import dataclass
from typing import List

from lzy.api.v2.servant.model.channel import Bindings
from lzy.api.v2.servant.model.zygote import Zygote


@dataclass
class TaskSpec:
    call_id: str
    operation_name: str
    zygote: Zygote
    bindings: Bindings
    dependent_calls: List[str]
