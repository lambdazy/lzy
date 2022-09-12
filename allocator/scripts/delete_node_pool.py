#!/usr/bin/env python3
import sys
from dataclasses import dataclass

from yandex.cloud.k8s.v1.node_group_service_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *

from common import *


@dataclass
class DeleteNodePoolConfig:
    env: str
    node_pool_id: str


def check_node_pool_with_id(node_group_service, node_pool_id):
    try:
        node_group_service.Get(
            GetNodeGroupRequest(
                node_group_id=node_pool_id
            )
        )
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.NOT_FOUND:
            raise Exception("k8s node pool {} is not exist\n".format(node_pool_id))


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else 'delete_node_pool_config.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, DeleteNodePoolConfig)

    sdk = create_sdk()
    node_group_service = sdk.client(NodeGroupServiceStub)

    # ------------ K8S NODE POOL EXISTENCE CHECK ------------ #
    check_node_pool_with_id(node_group_service, config.node_pool_id)

    ans = input("Are you sure you want to create node pool with id {}? (print 'YES!'): ".format(config.node_pool_id))
    if ans != "YES!":
        sys.exit()

    # ------------ K8S CLUSTER ------------ #
    print("trying to delete k8s node pool {}...\n".format(config.node_pool_id))
    node_group_service.Delete(
        DeleteNodeGroupRequest(
            node_group_id=config.node_pool_id
        )
    )
    print("k8s node pool {} was started deleting".format(config.node_pool_id))
