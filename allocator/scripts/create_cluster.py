#!/usr/bin/env python3
import sys
import os
import argparse
from dataclasses import dataclass
from subprocess import Popen, PIPE
import yandexcloud
from google.rpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.security_group_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import *
from yandex.cloud.k8s.v1.cluster_service_pb2 import *
from yandex.cloud.k8s.v1.cluster_pb2 import *

from common import *

@dataclass
class CreateClusterConfig:
    env: str
    folder_id: str
    subnet_id: str
    service_account_id: str
    cluster_name: str
    cluster_label: str


with open('create_cluster_config.yaml', 'r') as file:
    data = file.read()
config = strict_load_yaml(data, CreateClusterConfig)

sdk = yandexcloud.SDK(iam_token=os.environ['YC_TOKEN'])
subnet_service = sdk.client(SubnetServiceStub)
sg_service = sdk.client(SecurityGroupServiceStub)
cluster_service = sdk.client(ClusterServiceStub)

subnet = subnet_service.Get(GetSubnetRequest(subnet_id=config.subnet_id))
network_id = subnet.network_id
subnet_v4_cidrs = ",".join(subnet.v4_cidr_blocks)
zone_id = subnet.zone_id

ans = input("Are you sure you want to create cluster with this configuration? (print 'YES!'): ")
if ans != "YES!":
    sys.exit()

# ------------ K8S CLUSTER EXISTENCE CHECK ------------ #
clusters = list(filter(
    lambda c: c.name == config.cluster_name,
    cluster_service.List(
        ListClustersRequest(
            folder_id=config.folder_id
        )
    ).clusters
))
if len(clusters) > 0:
    print("k8s cluster {} in folder {} is already exist\n".format(config.cluster_name, config.folder_id))
    print("k8s cluster was NOT created!")
    sys.exit(1)

# ------------ SECURITY GROUPS ------------ #
# Source docs for security groups: https://cloud.yandex.ru/docs/managed-kubernetes/operations/connect/security-groups

try:
    # for basic cluster efficiency
    print("trying to create lzy-{}-main-sg security group...\n".format(config.cluster_name))
    operation = sg_service.Create(
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name="lzy-{}-main-sg".format(config.cluster_name),
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=0, to_port=65535),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["198.18.235.0/24", "198.18.248.0/24"])
                ),
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=0, to_port=65535),
                    protocol_name="any",
                    predefined_target="self_security_group"
                ),
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=0, to_port=65535),
                    protocol_name="any",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=[subnet_v4_cidrs])
                ),
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.EGRESS,
                    ports=PortRange(from_port=0, to_port=65535),
                    protocol_name="any",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/0"], v6_cidr_blocks=["0::/0"])
                ),
            ]
        )
    )
    print("successfully created lzy-{}-main-sg security group...\n".format(config.cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        print("lzy-{}-main-sg is already exist\n".format(config.cluster_name))
    else:
        print(e)
        sys.exit()
security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
main_sg_id = list(filter(lambda sg: sg.name == "lzy-{}-main-sg".format(config.cluster_name), security_groups))[0].id

# TODO: RESTRICT V4 AND V6 CIDRS!!!!!!!!!
try:
    # for access to running services from yandex subnets
    print("trying to create lzy-{}-public-services security group...\n".format(config.cluster_name))
    operation = sg_service.Create(
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name="lzy-{}-public-services".format(config.cluster_name),
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=30000, to_port=32767),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/24"], v6_cidr_blocks=["0::/0"])
                ),
            ]
        )
    )
    print("successfully created lzy-{}-public-services security group...\n".format(config.cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        print("lzy-{}-public-services is already exist\n".format(config.cluster_name))
    else:
        print(e)
        sys.exit()
security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
public_services_sg_id = list(filter(
    lambda sg: sg.name == "lzy-{}-public-services".format(config.cluster_name),
    security_groups
))[0].id

try:
    # for k8s api access from everywhere (0.0.0.0/0)
    print("trying to create lzy-{}-master-whitelist security group...\n".format(config.cluster_name))
    operation = sg_service.Create(
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name="lzy-{}-master-whitelist".format(config.cluster_name),
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=443, to_port=443),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/24"])
                ),
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=6443, to_port=6443),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/24"])
                ),
            ]
        )
    )
    print("successfully created lzy-{}-master-whitelist security group...\n".format(config.cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        print("lzy-{}-master-whitelist is already exist\n".format(config.cluster_name))
    else:
        print(e)
        sys.exit()
security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
master_whitelist_sg_id = list(filter(
    lambda sg: sg.name == "lzy-{}-master-whitelist".format(config.cluster_name),
    security_groups
))[0].id

# ------------ K8S CLUSTER ------------ #
try:
    print("trying to create {} k8s cluster...\n".format(config.cluster_name))
    cluster_service.Create(
        CreateClusterRequest(
            folder_id=config.folder_id,
            name=config.cluster_name,
            description="bla bla lba",
            labels={
                "lzy-node-pool-type": config.cluster_label
            },
            network_id=network_id,
            master_spec=MasterSpec(
                zonal_master_spec=ZonalMasterSpec(
                    zone_id=zone_id,
                    internal_v4_address_spec=InternalAddressSpec(
                        subnet_id=config.subnet_id
                    )
                ),
                security_group_ids=[main_sg_id, master_whitelist_sg_id]
            ),
            ip_allocation_policy=IPAllocationPolicy(
                cluster_ipv4_cidr_block="10.20.0.0/16",
                cluster_ipv6_cidr_block="fc00::/96",
                service_ipv6_cidr_block="fc01::/112"
            ),
            service_account_id=config.service_account_id,
            node_service_account_id=config.service_account_id,
            release_channel=ReleaseChannel.RAPID,
            cilium=Cilium(routing_mode=Cilium.RoutingMode.TUNNEL)
        )
    )
    print("k8s cluster {} was started creating".format(config.cluster_name))
except grpc.RpcError as e:
    if e.code() is grpc.StatusCode.ALREADY_EXISTS:
        # TODO: check cluster with same name existence before creating SGs
        print("k8s cluster {} in folder {} is already exist\n".format(config.cluster_name, config.folder_id))
        print("k8s cluster was NOT created!")
        exit(1)
    else:
        raise e
