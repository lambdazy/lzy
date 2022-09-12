#!/usr/bin/env python3
import os
import sys
from dataclasses import dataclass

import yandexcloud
from yandex.cloud.k8s.v1.cluster_service_pb2 import *
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import *
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import *

from common import *


@dataclass
class DeleteClusterConfig:
    env: str
    cluster_id: str


def check_cluster_with_id(cluster_service, cluster_id):
    try:
        cluster_service.Get(
            GetClusterRequest(
                cluster_id=cluster_id
            )
        )
    except grpc.RpcError as e:
        if e.code() is grpc.StatusCode.NOT_FOUND:
            raise Exception("k8s cluster {} is not exist\n".format(cluster_id))


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else 'delete_cluster_config.yaml'
    with open(filepath, 'r') as file:
        data = file.read()
    config = strict_load_yaml(data, DeleteClusterConfig)

    sdk = yandexcloud.SDK(iam_token=os.environ['YC_TOKEN'])
    cluster_service = sdk.client(ClusterServiceStub)

    # ------------ K8S CLUSTER EXISTENCE CHECK ------------ #
    check_cluster_with_id(cluster_service, config.cluster_id)

    ans = input("Are you sure you want to delete cluster with id {}? (print 'YES!'): ".format(config.cluster_id))
    if ans != "YES!":
        sys.exit()

    # ------------ K8S CLUSTER ------------ #
    print("trying to delete k8s cluster {}...\n".format(config.cluster_id))
    cluster_service.Delete(
        DeleteClusterRequest(
            cluster_id=config.cluster_id
        )
    )
    print("k8s cluster {} was started deleting".format(config.cluster_id))
