#!/usr/bin/env python3
from dataclasses import dataclass
from google.protobuf.field_mask_pb2 import FieldMask
from yandex.cloud.k8s.v1.node_group_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *

from common import *

LOG = logging.getLogger(__name__)


@dataclass
class SetNodePoolStateConfig:
    env: str
    node_pool_id: str
    active: bool


def find_node_pool_with_id(node_group_service, node_pool_id):
    try:
        return node_group_service.Get(
            GetNodeGroupRequest(
                node_group_id=node_pool_id
            )
        )
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.NOT_FOUND:
            raise Exception("k8s node pool {} is not exist\n".format(node_pool_id))


if __name__ == "__main__":
    format_logs()

    filepath = sys.argv[1] if len(sys.argv) > 1 else 'set_node_pool_state.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, SetNodePoolStateConfig)

    sdk = create_sdk()
    node_group_service = sdk.client(NodeGroupServiceStub)

    # ------------ K8S NODE POOL EXISTENCE CHECK ------------ #
    node_pool = find_node_pool_with_id(node_group_service, config.node_pool_id)



    ans = input("Are you sure you want to {} node pool {}? (print 'YES!'): "
                .format("activate" if config.active else "deactivate", config.node_pool_id))
    if ans != "YES!":
        sys.exit()

    new_labels = node_pool.node_labels
    new_state = "ACTIVE" if config.active else "INACTIVE"
    new_labels["lzy.ai/node-pool-state"] = new_state

    # ------------ K8S CLUSTER ------------ #
    LOG.info("trying to resize k8s node pool {}...\n".format(config.node_pool_id))
    node_group_service.Update(
        UpdateNodeGroupRequest(
            node_group_id=config.node_pool_id,
            update_mask=FieldMask(
                paths=["node_labels"]
            ),
            node_labels=new_labels
        )
    )
    LOG.info("k8s node pool {} has changed state to '{}'".format(config.node_pool_id, new_state))
