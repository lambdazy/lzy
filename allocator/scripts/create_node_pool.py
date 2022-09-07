#!/usr/bin/env python3
import os

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
node_pool_tains = input('node group taints for pool vms (for example, sku=gpu:NoSchedule for gpu vms): ')
platform = input('platform for pool vms: ')
cpu_count = input('cpu count for pool vms: ')
gpu_count = input('gpu count for pool vms: ')
disc_size = input('disc size for pool vms: ')
disc_type = input('disc type for pool vms: ')
memory = input('memory for pool vms: ')

ans = input("Are you sure you want to create node group with this configuration? (print 'YES!!!!!'): ")
if ans != "YES!!!!!":
    os.exit()

# ------------ SECURITY GROUPS ------------ #
main_sg_id = os.system(
    "yc vpc sg get --cloud-id {} --folder-id {} --name {}-main-sg --format json | jq -r '.id'".format(
        cloud_id,
        folder_id,
        cluster_name
    )
)
public_services_sg_id = os.system(
    "yc vpc sg get --cloud-id {} --folder-id {} --name {}-public-services --format json | jq -r '.id'".format(
        cloud_id,
        folder_id,
        cluster_name
    )
)

# ------------ NODE POOL ------------ #
os.system(
    "yc managed-kubernetes node-group create --help \
  --cloud-id {} \
  --folder-id {} \
  --cluster-name {} \
  --labels lzy.ai/node-pool-id={} \
  --labels lzy.ai/node-pool-label={} \
  --labels lzy.ai/node-pool-kind={} \
  --labels lzy.ai/node-pool-az={} \
  --labels lzy.ai/node-pool-state={} \
  --node-taints {} \
  --network-interface security-group-ids={},{},subnets={},ipv4-address=auto,ipv6-address=true \
  --platform {} \
  --cores {} \
  --gpus {} \
  --disk-size {} \
  --disk-type {} \
  --memory {} \
  --name {}".format(
        cloud_id,
        folder_id,
        cluster_name,
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
        memory,
        node_pool_name
    )
)
