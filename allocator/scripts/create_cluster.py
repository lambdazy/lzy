#!/usr/bin/env python3
import sys
from subprocess import Popen, PIPE


def process(command: str) -> str:
    p = Popen(command, shell=True, stdout=PIPE)
    out, err = p.communicate()
    return out.decode("utf-8")[:-1]


cloud_id = input('cloud id for pool vms: ')
folder_id = input('folder id for pool vms: ')
subnet_id = input('subnet id for pool vms: ')
network_id = process("yc vpc subnet get --id {} --format json | jq -r '.network_id'".format(subnet_id))
service_account_id = input("service account id for pool vms: ")
cluster_name = input('cluster name for pool vms: ')

ans = input("Are you sure you want to create cluster with this configuration? ")
if ans != "YES!!!!!":
    sys.exit()

# ------------ SECURITY GROUPS ------------ #
# Source docs for security groups: https://cloud.yandex.ru/docs/managed-kubernetes/operations/connect/security-groups

# for basic cluster efficiency
subnet_v4_cidrs = process("yc vpc subnet get --id {} --format json | jq -r '.v4_cidr_blocks | join(,)'".format(subnet_id))
process(
    "yc vpc sg create \
    --name {}-main-sg \
    --cloud-id {} \
    --folder-id {} \
    --network-id {} \
    --rule direction=ingress,port=0-65535,protocol=tcp,v4-cidrs=[198.18.235.0/24,198.18.248.0/24] \
    --rule direction=ingress,port=0-65535,protocol=any,predefined=self_security_group \
    --rule direction=ingress,port=0-65535,protocol=any,v4-cidrs=[{}] \
    --rule direction=egress,port=0-65535,protocol=any,v4-cidrs=[0.0.0.0/0],v6-cidrs=[0::/0]".format(
        cluster_name,
        cloud_id,
        folder_id,
        network_id,
        subnet_v4_cidrs
    )
)
main_sg_id = process(
    "yc vpc sg get --cloud-id {} --folder-id {} --name {}-main-sg --format json | jq -r '.id'".format(
        cloud_id,
        folder_id,
        cluster_name
    )
)

# TODO: RESTRICT V4 AND V6 CIDRS!!!!!!!!!
# for access to running services from yandex subnets
process(
    "yc vpc sg create \
    --name {}-public-services \
    --cloud-id {} \
    --folder-id {} \
    --network-id {} \
    --rule direction=ingress,port=30000-32767,protocol=tcp,v4-cidrs=[0.0.0.0/0],v6-cidrs=[0::/0]".format(
        cluster_name,
        cloud_id,
        folder_id,
        network_id,
    )
)
public_services_sg_id = process(
    "yc vpc sg get --cloud-id {} --folder-id {} --name {}-public-services --format json | jq -r '.id'".format(
        cloud_id,
        folder_id,
        cluster_name
    )
)

# for k8s api access from everywhere (0.0.0.0/0)
process(
    "yc vpc sg create \
    --name {}-master-whitelist \
    --cloud-id {} \
    --folder-id {} \
    --network-id {} \
    --rule direction=ingress,port=443,protocol=tcp,v4-cidrs=[0.0.0.0/0] \
    --rule direction=ingress,port=6443,protocol=tcp,v4-cidrs=[0.0.0.0/0]".format(
        cluster_name,
        cloud_id,
        folder_id,
        network_id,
    )
)
master_whitelist_sg_id = process(
    "yc vpc sg get --cloud-id {} --folder-id {} --name {}-master-whitelist --format json | jq -r '.id''".format(
        cloud_id,
        folder_id,
        cluster_name
    )
)

# ------------ K8S CLUSTER ------------ #
process(
    "yc managed-kubernetes cluster create --help \
  --cloud-id {} \
  --folder-id {} \
  --network-id {} \
  --subnet-id {} \
  --cluster-ipv4-range 10.20.0.0/16 \
  --cluster-ipv6-range fc00::/96 \
  --service-ipv6-range fc01::/112 \
  --dual-stack \
  --security-group-ids {},{} \
  --name {} \
  --public-ip \
  --cilium \
  --release-channel rapid \
  --service-account-id {} \
  --node-service-account-id {}".format(
        cloud_id,
        folder_id,
        network_id,
        subnet_id,
        main_sg_id,
        master_whitelist_sg_id,
        cluster_name,
        service_account_id,
        service_account_id
    )
)
