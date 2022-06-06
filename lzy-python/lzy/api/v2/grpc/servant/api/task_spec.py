from dataclasses import dataclass

from lzy.api.v2.servant.model.channel import Bindings
from lzy.api.v2.servant.model.zygote import Zygote


@dataclass
class TaskSpec:
    task_id: str
    zygote: Zygote
    bindings: Bindings
