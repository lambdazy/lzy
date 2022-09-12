#!/usr/bin/env python3
import sys
from dataclasses import dataclass

from yandex.cloud.k8s.v1.cluster_service_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import *
from yandex.cloud.k8s.v1.node_group_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2 import *
from yandex.cloud.k8s.v1.node_group_service_pb2_grpc import *
from yandex.cloud.k8s.v1.node_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2 import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *

from common import *
import logging

LOG = logging.getLogger(__name__)


@dataclass
class CreateNodePoolConfig:
    @dataclass
    class NodeTemplate:
        @dataclass
        class Resources:
            cpu_count: int
            gpu_count: int
            memory: int

        subnet_id: str
        platform_id: str
        disc_size: int
        disc_type: str
        resources: Resources

    @dataclass
    class NodeLabels:
        node_pool_id: str
        node_pool_label: str
        node_pool_kind: str

    @dataclass
    class ScalePolicy:
        min_pool_size: int
        max_pool_size: int

    env: str
    folder_id: str
    cluster_id: str
    node_pool_name: str
    node_template: NodeTemplate
    node_labels: NodeLabels
    node_pool_taints: str
    scale_policy: ScalePolicy


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


def get_cluster_name(cluster_id: str):
    try:
        cluster = cluster_service.Get(
            GetClusterRequest(
                cluster_id=cluster_id
            )
        )
        return cluster.name
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.NOT_FOUND:
            raise Exception("k8s cluster {} was not found\n".format(cluster_id))
        else:
            raise e


def get_security_group_id(sg_service: SecurityGroupService, name: str):
    sgs = list(filter(
        lambda sg: sg.name == name,
        sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
    ))
    if len(sgs) == 0:
        raise Exception("{} security group does not exist. (It must be created during script "
                        "create_cluster.py --cluster-name {} ...)".format(name, cluster_name))
    return sgs[0].id


if __name__ == "__main__":
    format_logs()

    filepath = sys.argv[1] if len(sys.argv) > 1 else 'create_node_pool_config.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, CreateNodePoolConfig)

    node_pool_taints = map(parse_taint, config.node_pool_taints[1:-1].split(","))
    memory = config.node_template.resources.memory * (1024 ** 3)  # GBs to Bytes
    disc_size = config.node_template.disc_size * (1024 ** 3)  # GBs to Bytes

    sdk = create_sdk()
    subnet_service = sdk.client(SubnetServiceStub)
    sg_service = sdk.client(SecurityGroupServiceStub)
    cluster_service = sdk.client(ClusterServiceStub)
    node_group_service = sdk.client(NodeGroupServiceStub)

    # ------------ K8S CLUSTER NAME ------------ #
    cluster_name = get_cluster_name(config.cluster_id)

    # ------------ SECURITY GROUPS ------------ #
    subnet = subnet_service.Get(GetSubnetRequest(subnet_id=config.node_template.subnet_id))
    main_sg_id = get_security_group_id(
        sg_service,
        "lzy-{}-main-sg".format(cluster_name)
    )
    public_services_sg_id = get_security_group_id(
        sg_service,
        "lzy-{}-public-services".format(cluster_name)
    )

    ans = input(
        "Are you sure you want to create node pool with this configuration? (print 'YES!'):\n{}\n".format(config))
    if ans != "YES!":
        sys.exit()

    # ------------ NODE POOL ------------ #
    try:
        LOG.info("trying to create {} k8s node group...\n".format(cluster_name))
        node_group_service.Create(
            CreateNodeGroupRequest(
                cluster_id=config.cluster_id,
                name=config.node_pool_name,
                description="K8s node pool for Lzy with lzy.ai/node-pool-label={}, lzy.ai/node-pool-kind={}".format(
                    config.node_labels.node_pool_label, config.node_labels.node_pool_kind
                ),
                labels={
                    # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
                },
                node_template=NodeTemplate(
                    labels={
                        # TODO: forward compute labels for Seva (https://st.yandex-team.ru/CLOUD-111652)
                    },
                    platform_id=config.node_template.platform_id,
                    resources_spec=ResourcesSpec(
                        memory=memory,
                        cores=config.node_template.resources.cpu_count,
                        gpus=config.node_template.resources.gpu_count
                    ),
                    boot_disk_spec=DiskSpec(
                        disk_type_id=config.node_template.disc_type,
                        disk_size=disc_size
                    ),
                    metadata={
                        # TODO: forward compute metadata for Seva (https://st.yandex-team.ru/CLOUD-111652)
                    },
                    network_interface_specs=[
                        NetworkInterfaceSpec(
                            subnet_ids=[config.node_template.subnet_id],
                            primary_v4_address_spec=NodeAddressSpec(),
                            primary_v6_address_spec=NodeAddressSpec(),
                            security_group_ids=[main_sg_id, public_services_sg_id]
                        )
                    ]
                ),
                scale_policy=ScalePolicy(
                    auto_scale=ScalePolicy.AutoScale(
                        min_size=config.scale_policy.min_pool_size,
                        max_size=config.scale_policy.max_pool_size,
                        initial_size=config.scale_policy.min_pool_size
                    )
                ),
                node_taints=node_pool_taints,
                node_labels={
                    "lzy.ai/node-pool-id": config.node_labels.node_pool_id,
                    "lzy.ai/node-pool-label": config.node_labels.node_pool_label,
                    "lzy.ai/node-pool-kind": config.node_labels.node_pool_kind,
                    "lzy.ai/node-pool-az": subnet.zone_id,
                    "lzy.ai/node-pool-state": "ACTIVE"  # or "INACTIVE" ???
                }
            )
        )
        LOG.info(
            "k8s node group {} in cluster {} ({}) was started creating".format(config.node_pool_name, config.cluster_id,
                                                                               cluster_name))
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.ALREADY_EXISTS:
            LOG.error("k8s node group {} in folder {} is already exist\n".format(config.node_pool_name, config.folder_id))
            LOG.error("k8s node group was NOT created!")
        else:
            raise e
