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
class ResizeNodePoolConfig:
    env: str
    node_pool_id: str
    new_min_pool_size: int
    new_max_pool_size: int


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

    filepath = sys.argv[1] if len(sys.argv) > 1 else 'resize_node_pool_config.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, ResizeNodePoolConfig)

    sdk = create_sdk()
    node_group_service = sdk.client(NodeGroupServiceStub)

    # ------------ K8S NODE POOL EXISTENCE CHECK ------------ #
    find_node_pool_with_id(node_group_service, config.node_pool_id)

    ans = input("Are you sure you want to resize node pool {} with min size = {}, max size = {}? (print 'YES!'): "
                .format(config.node_pool_id, config.new_min_pool_size, config.new_max_pool_size))
    if ans != "YES!":
        sys.exit()

    # ------------ K8S CLUSTER ------------ #
    LOG.info("trying to resize k8s node pool {}...\n".format(config.node_pool_id))
    node_group_service.Update(
        UpdateNodeGroupRequest(
            node_group_id=config.node_pool_id,
            update_mask=FieldMask(
                paths=["scale_policy.auto_scale.min_size",
                       "scale_policy.auto_scale.max_size",
                       "scale_policy.auto_scale.initial_size"]
            ),
            scale_policy=ScalePolicy(
                auto_scale=ScalePolicy.AutoScale(
                    min_size=config.new_min_pool_size,
                    max_size=config.new_max_pool_size,
                    initial_size=config.new_min_pool_size
                )
            )
        )
    )
    LOG.info("k8s node pool {} was started resizing".format(config.node_pool_id))
