from typing import List
from uuid import uuid4

from lzy.api.v2.api.lzy_call import LzyCall
from lzy.api.v2.proxy_adapter import is_lzy_proxy

from lzy.proto.bet.priv.v2.graph import (
    GraphExecuteRequest,
    TaskDesc,
    SlotToChannelAssignment,
)


def prepare_task(call: LzyCall) -> TaskDesc:
    loc_args, non_loc_args = [], []
    for name, arg in call.named_arguments():
        slot_name = f"{call.description}:{name}"
        if is_lzy_proxy(arg):
            non_loc_args.append(slot_name)
        else:
            loc_args.append(slot_name)

    slot_assignments = [
        SlotToChannelAssignment(s_name, str(uuid4())) for s_name in non_loc_args
    ]
    return TaskDesc(
        str(uuid4()),
        call.zygote,
        slot_assignments=slot_assignments,
    )


def build_graph_execute_request(
    wflow_id: str,
    tasks: List[LzyCall],
) -> GraphExecuteRequest:
    return GraphExecuteRequest(
        workflow_id=wflow_id,
        tasks=[prepare_task(task) for task in tasks],
        # TODO[ottergottaott]: ChannelDescription
    )
