#!/usr/bin/env python3
import os
import sys
import argparse
from dataclasses import dataclass

import yandexcloud
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.security_group_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import *
from yandex.cloud.k8s.v1.cluster_service_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2_grpc import *
from yandex.cloud.k8s.v1.node_group_service_pb2 import *
from yandex.cloud.k8s.v1.node_group_pb2 import *
from yandex.cloud.k8s.v1.node_pb2 import *
from common import *


@dataclass
class CreateNodePoolConfig:
    env: str
    folder_id: str
    cluster_id: str
    subnet_id: str
    service_account_id: str
    node_pool_name: str
    node_pool_id: str
    node_pool_label: str
    node_pool_kind: str
    node_pool_taints: str
    min_pool_size: int
    max_pool_size: int
    platform_id: str
    cpu_count: int
    gpu_count: int
    disc_size: int
    disc_type: str
    memory: int


with open('create_node_pool_config.yaml', 'r') as file:
    data = file.read()
config = strict_load_yaml(data, CreateNodePoolConfig)


def parse_taint(taint_str: str):
    key, part_2 = taint_str.split("=")
    value, effect_str = part_2.split(":")
    effect = Taint.Effect.NO_SCHEDULE if effect_str == "NoSchedule" \
        else Taint.Effect.NO_EXECUTE if effect_str == "NoExecute" \
        else Taint.Effect.PREFER_NO_SCHEDULE if effect_str == "NoExecute" \
        else None
    if effect is None:
        raise ValueError("cannot parse k8s taint: {}".format(taint_str))
    return Taint(
        key=key,
        value=value,
        effect=effect
    )


node_pool_taints = map(parse_taint, config.node_pool_taints[1:-1].split(","))
memory = config.memory * (1024 ** 3)  # GBs to Bytes
disc_size = config.disc_size * (1024 ** 3)  # GBs to Bytes

sdk = yandexcloud.SDK(iam_token=os.environ['YC_TOKEN'])
subnet_service = sdk.client(SubnetServiceStub)
sg_service = sdk.client(SecurityGroupServiceStub)
cluster_service = sdk.client(ClusterServiceStub)
node_group_service = sdk.client(NodeGroupServiceStub)

ans = input("Are you sure you want to create node group with this configuration? (print 'YES!'): ")
if ans != "YES!":
    sys.exit()

# ------------ K8S CLUSTER EXISTENCE CHECK ------------ #
try:
    cluster = cluster_service.Get(
        GetClusterRequest(
            cluster_id=config.cluster_id
        )
    )
    cluster_name = cluster.name
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.NOT_FOUND:
        # TODO: check cluster with same name existence before creating SGs
        print("k8s cluster with id {} was not found!\n".format(config.cluster_id))
        exit(1)
    else:
        raise e

# ------------ SECURITY GROUPS ------------ #
subnet = subnet_service.Get(GetSubnetRequest(subnet_id=config.subnet_id))

sgs = list(filter(
    lambda sg: sg.name == "lzy-{}-main-sg".format(cluster_name),
    sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
))
if len(sgs) == 0:
    raise Exception("lzy-{}-main-sg security group does not exist. (It must be created during script "
                    "create_cluster.py --cluster-name {} ...)".format(cluster_name, cluster_name))
main_sg_id = sgs[0].id

sgs = list(filter(
    lambda sg: sg.name == "lzy-{}-public-services".format(cluster_name),
    sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
))
if len(sgs) == 0:
    raise Exception("lzy-{}-public-services security group does not exist. (It must be created during script "
                    "create_cluster.py --cluster-name {} ...)".format(cluster_name, cluster_name))
public_services_sg_id = sgs[0].id

# ------------ NODE POOL ------------ #
try:
    print("trying to create {} k8s node group...\n".format(cluster_name))
    node_group_service.Create(
        CreateNodeGroupRequest(
            cluster_id=config.cluster_id,
            name=config.node_pool_name,
            description="",
            labels={
                # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
            },
            node_template=NodeTemplate(
                labels={
                    # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
                },
                platform_id=config.platform_id,
                resources_spec=ResourcesSpec(
                    memory=memory,
                    cores=config.cpu_count,
                    gpus=config.gpu_count
                ),
                boot_disk_spec=DiskSpec(
                    disk_type_id=config.disc_type,
                    disk_size=disc_size
                ),
                metadata={
                    # TODO: forward compute metadata for Seva (https://st.yandex-team.ru/CLOUD-111652)
                },
                network_interface_specs=[
                    NetworkInterfaceSpec(
                        subnet_ids=[config.subnet_id],
                        primary_v4_address_spec=NodeAddressSpec(),
                        primary_v6_address_spec=NodeAddressSpec(),
                        security_group_ids=[main_sg_id, public_services_sg_id]
                    )
                ]
            ),
            scale_policy=ScalePolicy(
                auto_scale=ScalePolicy.AutoScale(
                    min_size=config.min_pool_size,
                    max_size=config.max_pool_size,
                    initial_size=config.min_pool_size
                )
            ),
            node_taints=node_pool_taints,
            node_labels={
                "lzy.ai/node-pool-id": config.node_pool_id,
                "lzy.ai/node-pool-label": config.node_pool_label,
                "lzy.ai/node-pool-kind": config.node_pool_kind,
                "lzy.ai/node-pool-az": subnet.zone_id,
                "lzy.ai/node-pool-state": "ACTIVE"  # or "INACTIVE" ???
            }
        )
    )
    print("k8s node group {} in cluster {} ({}) was started creating".format(config.node_pool_name, config.cluster_id,
                                                                             cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        # TODO: check cluster with same name existence before creating SGs
        print("k8s node group {} in folder {} is already exist\n".format(config.node_pool_name, config.folder_id))
        print("k8s node group was NOT created!")
        exit(1)
    else:
        raise e
    raise e
