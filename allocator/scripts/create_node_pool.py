#!/usr/bin/env python3
import os
import sys
import argparse

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


parser = argparse.ArgumentParser(description="create k8s node pool for Lzy Allocator")
parser.add_argument("--folder-id", type=str, nargs="?", action="store")
# TODO: allow specifying either --cluster-id or --cluster-name
parser.add_argument("--cluster-id", type=str, nargs="?", action="store")
# TODO: take subnet form k8s master ??
parser.add_argument("--subnet-id", type=str, nargs="?", action="store")
parser.add_argument("--service-account-id", type=str, nargs="?", action="store")
parser.add_argument("--node-pool-name", type=str, nargs="?", action="store")
parser.add_argument("--node-pool-id", type=str, nargs="?", action="store")
parser.add_argument("--node-pool-label", type=str, nargs="?", action="store")
parser.add_argument("--node-pool-kind", type=str, nargs="?", action="store")
parser.add_argument("--node-pool-tains", type=str, nargs="?", action="store", default=[],
                    help="[<key>=<value>:(NoSchedule | NoExecute | PreferNoSchedule), ...] (for example, [sku=gpu:NoSchedule] for gpu machines")
parser.add_argument("--platform-id", type=str, nargs="?", action="store")
parser.add_argument("--cpu-count", type=int, nargs="?", action="store")
parser.add_argument("--min-pool-size", type=int, nargs="?", action="store")
parser.add_argument("--max-pool-size", type=int, nargs="?", action="store")
parser.add_argument("--gpu-count", type=int, nargs="?", action="store")
parser.add_argument("--disc-size", type=int, nargs="?", action="store")
parser.add_argument("--disc-type", type=str, nargs="?", action="store",
                    help="network-ssd|network-hdd|network-ssd-nonreplicated")
parser.add_argument("--memory", type=int, nargs="?", action="store")

args = parser.parse_args()

# May be just use args.folder_id, args.node_pool_name in code to cut this shit?
folder_id = args.folder_id
cluster_id = args.cluster_id
subnet_id = args.subnet_id
service_account_id = args.service_account_id
node_pool_name = args.node_pool_name
node_pool_id = args.node_pool_id
node_pool_label = args.node_pool_label
node_pool_kind = args.node_pool_kind
node_pool_taints_str = args.node_pool_tains
node_pool_taints = map(parse_taint, node_pool_taints_str[1:-1].split(","))
min_pool_size = args.min_pool_size
max_pool_size = args.max_pool_size
platform_id = args.platform_id
cpu_count = args.cpu_count
gpu_count = args.gpu_count
disc_size = args.disc_size * (1024 ** 3)  # GBs to Bytes
disc_type = args.disc_type
memory = args.memory * (1024 ** 3)  # GBs to Bytes

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
            cluster_id=cluster_id
        )
    )
    cluster_name = cluster.name
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.NOT_FOUND:
        # TODO: check cluster with same name existence before creating SGs
        print("k8s cluster with id {} was not found!\n".format(cluster_id))
        exit(1)
    else:
        raise e

# ------------ SECURITY GROUPS ------------ #
subnet = subnet_service.Get(GetSubnetRequest(subnet_id=subnet_id))

sgs = list(filter(
    lambda sg: sg.name == "lzy-{}-main-sg".format(cluster_name),
    sg_service.List(ListSecurityGroupsRequest(folder_id=folder_id)).security_groups
))
if len(sgs) == 0:
    raise Exception("lzy-{}-main-sg security group does not exist. (It must be created during script "
                    "create_cluster.py --cluster-name {} ...)".format(cluster_name, cluster_name))
main_sg_id = sgs[0].id

sgs = list(filter(
    lambda sg: sg.name == "lzy-{}-public-services".format(cluster_name),
    sg_service.List(ListSecurityGroupsRequest(folder_id=folder_id)).security_groups
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
            cluster_id=cluster_id,
            name=node_pool_name,
            description="",
            labels={
                # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
            },
            node_template=NodeTemplate(
                labels={
                    # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
                },
                platform_id=platform_id,
                resources_spec=ResourcesSpec(
                    memory=memory,
                    cores=cpu_count,
                    gpus=gpu_count
                ),
                boot_disk_spec=DiskSpec(
                    disk_type_id=disc_type,
                    disk_size=disc_size
                ),
                metadata={
                    # TODO: forward compute metadata for Seva (https://st.yandex-team.ru/CLOUD-111652)
                },
                network_interface_specs=[
                    NetworkInterfaceSpec(
                        subnet_ids=[subnet_id],
                        primary_v4_address_spec=NodeAddressSpec(),
                        primary_v6_address_spec=NodeAddressSpec(),
                        security_group_ids=[main_sg_id, public_services_sg_id]
                    )
                ]
            ),
            scale_policy=ScalePolicy(
                auto_scale=ScalePolicy.AutoScale(
                    min_size=min_pool_size,
                    max_size=max_pool_size,
                    initial_size=min_pool_size
                )
            ),
            node_taints=node_pool_taints,
            node_labels={
                "lzy.ai/node-pool-id": node_pool_id,
                "lzy.ai/node-pool-label": node_pool_label,
                "lzy.ai/node-pool-kind": node_pool_kind,
                "lzy.ai/node-pool-az": subnet.zone_id,
                "lzy.ai/node-pool-state": "ACTIVE"  # or "INACTIVE" ???
            }
        )
    )
    print("k8s node group {} in cluster {} ({}) was started creating".format(node_pool_name, cluster_id, cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        # TODO: check cluster with same name existence before creating SGs
        print("k8s node group {} in folder {} is already exist\n".format(node_pool_name, folder_id))
        print("k8s node group was NOT created!")
        exit(1)
    else:
        raise e
    raise e
