#!/usr/bin/env python3
import sys
from subprocess import Popen, PIPE

cloud_id = input('cloud id for pool vms: ')
folder_id = input('folder id for pool vms: ')
subnet_id = input('subnet id for pool vms: ')
service_account_id = input("service account id for pool vms: ")
cluster_name = input('cluster name for pool vms: ')
node_pool_name = input('node group name for pool vms: ')
node_pool_id = input('node group id for pool vms: ')
node_pool_label = input('node group label for pool vms: ')
node_pool_kind = input('node group kind for pool vms: ')
node_pool_az = input('node group az for pool vms: ')
node_pool_state = input('node group state for pool vms: ')
node_pool_tains = input('node group taints for pool vms (for example, sku=gpu:NoSchedule for gpu vms, or empty): ')
platform = input('platform for pool vms: ')
cpu_count = input('cpu count for pool vms: ')
gpu_count = input('gpu count for pool vms: ')
disc_size = input('disc size for pool vms: ')
disc_type = input('disc type for pool vms (must be >= 30: ')
memory = input('memory for pool vms: ')

ans = input("Are you sure you want to create node group with this configuration? (print 'YES!'): ")
if ans != "YES!":
    sys.exit()

# ------------ SECURITY GROUPS ------------ #
# main_sg_id = process(
#     "yc vpc sg get --cloud-id {} --folder-id {} --name lzy-{}-main-sg --format json | jq -r '.id'".format(
#         cloud_id,
#         folder_id,
#         cluster_name
#     )
# )
security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=folder_id)).security_groups
main_sg_id = list(filter(
    lambda sg: sg.name == "lzy-{}-main-sg".format(cluster_name),
    security_groups
))[0].id

# public_services_sg_id = process(
#     "yc vpc sg get --cloud-id {} --folder-id {} --name lzy-{}-public-services --format json | jq -r '.id'".format(
#         cloud_id,
#         folder_id,
#         cluster_name
#     )
# )
security_groups = sg_service.List(ListSecurityGroupsRequest(folder_id=folder_id)).security_groups
main_sg_id = list(filter(
    lambda sg: sg.name == "lzy-{}-public-services".format(cluster_name),
    security_groups
))[0].id

# TODO: fixed scale or auto scale
print("trying to create {} k8s node group...\n".format(cluster_name))
# ------------ NODE POOL ------------ #
process(
    "yc managed-kubernetes node-group create \
  --cloud-id {} \
  --folder-id {} \
  --cluster-name {} \
  --name {} \
  --node-labels lzy.ai/node-pool-id={} \
  --node-labels lzy.ai/node-pool-label={} \
  --node-labels lzy.ai/node-pool-kind={} \
  --node-labels lzy.ai/node-pool-az={} \
  --node-labels lzy.ai/node-pool-state={} \
  --node-taints \"{}\" \
  --network-interface security-group-ids=[{},{}],subnets={},ipv4-address=auto,ipv6-address=auto \
  --platform {} \
  --cores {} \
  --gpus {} \
  --disk-size {} \
  --disk-type {} \
  --fixed-size 2 \
  --memory {}".format(
        cloud_id,
        folder_id,
        cluster_name,
        node_pool_name,
        node_pool_id,
        node_pool_label,
        node_pool_kind,
        node_pool_az,
        node_pool_state,
        node_pool_tains,
        main_sg_id,
        public_services_sg_id,
        subnet_id,
        platform,
        cpu_count,
        gpu_count,
        disc_size,
        disc_type,
        memory
    )
)
