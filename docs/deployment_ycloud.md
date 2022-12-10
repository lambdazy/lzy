## Deployment

### Requirements

1) terraform
2) ycloud subscription
3) yc cli

### Steps

1) create terraform module
2) import [ycloud_common](../deployment/tf/modules/ycloud_common) module
3) yc login
4) create GitHub OAuth App
5) generate client secret
6) create tvars.json file with the following content

```
{
  "github-client-id": "",
  "github-secret": ""
}
```

7) copy client ID and client secret from GitHub OAuth App and paste to the corresponding json fields
8) terraform init
9) terraform plan -var-file=tvars.json -out=tfplan
10) terraform apply tfplan

### View logs

**Requirements**
* `yc managed-kubernetes cluster get-credentials --id <cluster-id> --external --context-name lzy` to create context
* `kubectl config use-context lzy` to use context

**Server logs online**
* `kubectl get pods` to find out server pod name (looks like `lzy-server-<id>`)
* `kubectl get logs -f lzy-server-<id>`

**Worker logs online**
* `kubectl get pods` to find out worker pod name (looks like `lzy-worker-<id>`)
* `kubectl exec -it lzy-worker-<id> -- cat /tmp/lzy-log/worker/worker.log`

**Old server or worker logs**
* `kubectl get pods` to find out clickhouse pod name (looks like `clickhouse-<id>`)
* `kubectl port-forward clickhouse-<id> 8123:8123`
* Go to ycloud for credentials: YCloud folder -> lzy-cluster -> Configuration (you can search for it or find in left bar) -> Secrets tab -> clickhouse

### Example
See example [here](../deployment/tf/modules/ycloud_example)
