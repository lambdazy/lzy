#!/usr/bin/env python3
from dataclasses import dataclass
from yandex.cloud.k8s.v1.cluster_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import *
from yandex.cloud.vpc.v1.security_group_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2 import *
from yandex.cloud.vpc.v1.security_group_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2 import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *

from common import *

LOG = logging.getLogger(__name__)


@dataclass
class CreateClusterConfig:
    env: str
    folder_id: str
    subnet_id: str
    service_account_id: str
    cluster_name: str
    cluster_label: str


def check_cluster_with_same_name(cluster_service):
    clusters = list(filter(
        lambda c: c.name == config.cluster_name,
        cluster_service.List(
            ListClustersRequest(
                folder_id=config.folder_id
            )
        ).clusters
    ))
    if len(clusters) > 0:
        raise Exception("k8s cluster {} in folder {} is already exist\n".format(config.cluster_name, config.folder_id))


def get_create_or_security_group(config: CreateSecurityGroupRequest, name: str, req: CreateSecurityGroupRequest) -> str:
    security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=config.folder_id)).security_groups
    security_groups_with_same_name = list(filter(lambda sg: sg.name == name, security_groups))
    if len(security_groups_with_same_name) > 0:
        LOG.info("security group {} is already exist\n".format(name))
        return security_groups_with_same_name[0].id
    else:
        # for basic cluster efficiency
        LOG.info("trying to create {} security group...\n".format(name))
        operation = sg_service.Create(req)
        operation_result = sdk.wait_operation_and_get_result(
            operation=operation,
            response_type=SecurityGroup,
            meta_type=CreateSecurityGroupMetadata,
        )
        LOG.info("successfully created {} security group...\n".format(name))
        return operation_result.response.id


if __name__ == "__main__":
    format_logs()

    filepath = sys.argv[1] if len(sys.argv) > 1 else 'create_cluster_config.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, CreateClusterConfig)

    sdk = create_sdk()
    subnet_service = sdk.client(SubnetServiceStub)
    sg_service = sdk.client(SecurityGroupServiceStub)
    cluster_service = sdk.client(ClusterServiceStub)

    subnet = subnet_service.Get(GetSubnetRequest(subnet_id=config.subnet_id))
    LOG.info("subnet id {} was resolved to subnet \"{}\" in zone {} with v4 cidrs {} and v6 cidrs {}\n".format(
        config.subnet_id, subnet.name, subnet.zone_id, subnet.v4_cidr_blocks, subnet.v6_cidr_blocks
    ))
    network_id = subnet.network_id
    subnet_v4_cidrs = ",".join(subnet.v4_cidr_blocks)
    zone_id = subnet.zone_id

    # ------------ K8S CLUSTER EXISTENCE CHECK ------------ #
    check_cluster_with_same_name(cluster_service)

    ans = input("Are you sure you want to create cluster with this configuration? (print 'YES!'):\n{}\n".format(config))
    if ans != "YES!":
        sys.exit()

    # ------------ SECURITY GROUPS ------------ #
    # Source docs for security groups: https://cloud.yandex.ru/docs/managed-kubernetes/operations/connect/security-groups
    k8s_cluster_ipv4_cidr_block = "10.20.0.0/16"
    k8s_service_ipv4_cidr_block = "10.96.0.0/16"
    k8s_cluster_ipv6_cidr_block = "fc00::/96"
    k8s_service_ipv6_cidr_block = "fc01::/112"
    balancer_addresses_for_main_sg = ["198.18.235.0/24", "198.18.248.0/24"]

    main_sg_name = "lzy-{}-main-sg".format(config.cluster_name)
    main_sg_id = get_create_or_security_group(
        config,
        main_sg_name,
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name=main_sg_name,
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=0, to_port=65535),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=balancer_addresses_for_main_sg)
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
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=[k8s_cluster_ipv4_cidr_block, k8s_service_ipv4_cidr_block],
                                           v6_cidr_blocks=[k8s_cluster_ipv6_cidr_block, k8s_service_ipv6_cidr_block])
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

    # TODO: RESTRICT INGRESS V4 AND V6 CIDRS!!!!!!!!!
    public_svs_sg_name = "lzy-{}-public-services".format(config.cluster_name)
    yc_vpc_min_port = 30000
    yc_vpc_max_port = 32767
    public_services_sg_id = get_create_or_security_group(
        config,
        public_svs_sg_name,
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name=public_svs_sg_name,
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=yc_vpc_min_port, to_port=yc_vpc_max_port),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/0"], v6_cidr_blocks=["0::/0"])
                ),
            ]
        )
    )

    master_sg_name = "lzy-{}-master-whitelist".format(config.cluster_name)
    k8s_master_ports = [443, 6443]
    master_whitelist_sg_id = get_create_or_security_group(
        config,
        master_sg_name,
        CreateSecurityGroupRequest(
            folder_id=config.folder_id,
            name=master_sg_name,
            network_id=network_id,
            rule_specs=[
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=k8s_master_ports[0], to_port=k8s_master_ports[0]),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/0"])
                ),
                SecurityGroupRuleSpec(
                    direction=SecurityGroupRule.Direction.INGRESS,
                    ports=PortRange(from_port=k8s_master_ports[1], to_port=k8s_master_ports[1]),
                    protocol_name="tcp",
                    cidr_blocks=CidrBlocks(v4_cidr_blocks=["0.0.0.0/0"])
                ),
            ]
        )
    )

    # ------------ K8S CLUSTER ------------ #
    try:
        LOG.info("trying to create {} k8s cluster...\n".format(config.cluster_name))
        cluster_service.Create(
            CreateClusterRequest(
                folder_id=config.folder_id,
                name=config.cluster_name,
                description="K8s cluster for Lzy node pools with lzy-node-pool-type={}".format(config.cluster_label),
                labels={
                    "lzy-node-pool-type": config.cluster_label
                },
                network_id=network_id,
                master_spec=MasterSpec(
                    zonal_master_spec=ZonalMasterSpec(
                        zone_id=zone_id,
                        internal_v4_address_spec=InternalAddressSpec(
                            subnet_id=config.subnet_id
                        ),
                        external_v4_address_spec=ExternalAddressSpec()
                    ),
                    security_group_ids=[main_sg_id, master_whitelist_sg_id]
                ),
                ip_allocation_policy=IPAllocationPolicy(
                    cluster_ipv4_cidr_block=k8s_cluster_ipv4_cidr_block,
                    service_ipv4_cidr_block=k8s_service_ipv4_cidr_block,
                    cluster_ipv6_cidr_block=k8s_cluster_ipv6_cidr_block,
                    service_ipv6_cidr_block=k8s_service_ipv6_cidr_block
                ),
                service_account_id=config.service_account_id,
                node_service_account_id=config.service_account_id,
                release_channel=ReleaseChannel.RAPID,
                cilium=Cilium(routing_mode=Cilium.RoutingMode.TUNNEL)
            )
        )
        LOG.info("k8s cluster {} was started creating".format(config.cluster_name))
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.ALREADY_EXISTS:
            LOG.error("k8s cluster {} in folder {} is already exist\n".format(config.cluster_name, config.folder_id))
            LOG.error("k8s cluster was NOT created!")
        else:
            raise e
