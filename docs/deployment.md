## Deployment

### Requirements

1) terraform
2) azure subscription
3) azure cli

### Steps

1) create terraform module
2) import [azure_common](deployment/tf/modules/azure_common) module
3) az login
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
* `az aks get-credentials --name lzy-prod --resource-group lzy-prod --context lzy-prod` to create context
* `kubectl config use-context lzy-prod` to use context

**Server logs online**
* `kubectl get pods` to find out server pod name (looks like `lzy-server-<id>`)
* `kubectl get logs -f lzy-server-<id>`

**Servant logs online**
* `kubectl get pods` to find out servant pod name (looks like `lzy-servant-<id>`)
* `kubectl exec -it lzy-servant-<id> -- cat /tmp/lzy-log/servant/servant.log`

**Old server or servant logs**
* `kubectl get pods` to find out clickhouse pod name (looks like `clickhouse-<id>`)
* `kubectl port-forward clickhouse-<id> 8123:8123`
* Go to azure for credentials: Azure -> lzy-prod -> Configuration (you can search for it or find in left bar) -> Secrets tab -> clickhousse

### Example

In example, we use backend `azurerm` for remote terraform state storing

```
terraform {
  backend "azurerm" {
    resource_group_name  = "my-lzy-terraformstate"
    storage_account_name = "mylzytfstatestorage"
    container_name       = "terraformstate"
    key                  = "lzy.terraform.tfstate"
  }
}

module "azure_common" {
  source                     = "git@github.com:lambda-zy/lzy.git//deployment/tf/modules/azure_common"
  installation_name          = "my-lzy-installation"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "master"
  server-image               = "lzydock/lzy-server:master"
}
```

Same example, but here we use `local` backend

```
terraform {
  backend "local" {
  }
}

module "azure_common" {
  source                     = "git@github.com:lambda-zy/lzy.git//deployment/tf/modules/azure_common"
  installation_name          = "my-lzy-installation"
  oauth-github-client-id     = var.github-client-id
  oauth-github-client-secret = var.github-secret
  s3-postfics                = "master"
  server-image               = "lzydock/lzy-server:master"
}
```
