from kubernetes import client, config
from os import path
import yaml
import time

def get_pod(name, namespace, api):
    filtered = list(filter(lambda p: p.metadata.name == name, api.list_namespaced_pod(namespace=namespace).items))
    if len(filtered) > 0:
        return filtered[0]
    else:
        return None

def wait_pod_phase_or_destroy(name, namespace, api, phase):
    while True:
        print("waiting for pod %s to be in phase %s" % (name, phase))
        time.sleep(2)
        pod = get_pod(name, namespace, api)
        if pod is None:
            break
        if pod.status is None or pod.status.phase is None:
            continue
        if pod.status.phase == phase:
            break

def main():
    config.load_kube_config()
    api = client.CoreV1Api()

    default_namespace="default"

    kharon_pod_name="lzy-kharon"
    kharon_pod=get_pod(kharon_pod_name, default_namespace, api)
    if kharon_pod is not None:
        api.delete_namespaced_pod(name=kharon_pod_name, namespace=default_namespace)
    wait_pod_phase_or_destroy(kharon_pod_name, default_namespace, api, "")

    server_pod_name="lzy-server"
    server_pod=get_pod(server_pod_name, default_namespace, api)
    if server_pod is not None:
        api.delete_namespaced_pod(name=server_pod_name, namespace=default_namespace)
    wait_pod_phase_or_destroy(server_pod_name, default_namespace, api, "")

    backoffice_pod_name="lzy-backoffice"
    backoffice_pod=get_pod(backoffice_pod_name, default_namespace, api)
    if backoffice_pod is not None:
        api.delete_namespaced_pod(name=backoffice_pod_name, namespace=default_namespace)
    wait_pod_phase_or_destroy(backoffice_pod_name, default_namespace, api, "")

    with open(path.join("deployment", "lzy-server-pod-template.yaml")) as f:
        dep = yaml.safe_load(f)
        resp = api.create_namespaced_pod(body=dep, namespace=default_namespace)
        wait_pod_phase_or_destroy(server_pod_name, default_namespace, api, "Running")

    with open(path.join("deployment", "lzy-kharon-pod-template.yaml")) as f:
        dep = yaml.safe_load(f)
        lzy_server_ip = get_pod(server_pod_name, default_namespace, api).status.pod_ip
        dep["spec"]["containers"][0]["env"].append(client.models.V1EnvVar(name="LZY_SERVER_IP", value=lzy_server_ip))
        resp = api.create_namespaced_pod(body=dep, namespace=default_namespace)
        wait_pod_phase_or_destroy(kharon_pod_name, default_namespace, api, "Running")

    with open(path.join("deployment", "lzy-backoffice-pod-template.yaml")) as f:
        dep = yaml.safe_load(f)
        lzy_server_ip = get_pod(server_pod_name, default_namespace, api).status.pod_ip
        dep["spec"]["containers"][1]["env"].append(client.models.V1EnvVar(name="LZY_SERVER_IP", value=lzy_server_ip))
        resp = api.create_namespaced_pod(body=dep, namespace=default_namespace)
        wait_pod_phase_or_destroy(backoffice_pod_name, default_namespace, api, "Running")

    print("Listing pods with their IPs:")
    ret = api.list_namespaced_pod(namespace=default_namespace)
    for i in ret.items:
        print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

if __name__ == '__main__':
    main()
