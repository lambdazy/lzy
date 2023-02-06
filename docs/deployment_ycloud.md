## Deployment

### Requirements

1) terraform
2) Yandex Cloud subscription
3) YC cli
4) (optional) kubectl - for logs

### Steps

1) Create terraform module and import [v2](../deployment/tf/modules/v2) module (see example [here](../deployment/tf/modules/v2_example)).
It's necessary to instantiate "yandex" terraform provider in your module.
2) Perform yc login into your organization
3) Create GitHub OAuth App
4) Generate client secret
5) Create tvars.json file with the following content
```
{
  "oauth_github_client_id": "<your_client_id>",
  "oauth_github_client_secret": "<your_client_secret>",
  "docker_images_tag": "1.1", //optional, has default
  "docker_worker_image_tag": "1.2", //optional, has default
  "docker_unified_agent_tag": "1.0", //optional, has default
  "yc_token": "<yc_token>" //optional, see the step about authorized key
}
```

6) Copy client ID and client secret from GitHub OAuth App and paste to the corresponding json fields
7) Create [service account](https://cloud.yandex.com/en/docs/iam/concepts/users/service-accounts) with a role `admin` in your folder.
8) If you want to store your tfstate in YC's S3 bucket, create 'Static access key' in the service account and save ID and secret.
Then put them in the env variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
Or put them in your terraform module. For more information read [docs about S3 usage with terraform](https://developer.hashicorp.com/terraform/language/settings/backends/s3).
9) Create 'Authorized key' in your service account and save content of the key into local file.
Then put path to the file to env variable `YC_SERVICE_ACCOUNT_KEY_FILE`. It's necessary to access your YC cluster from terraform.
<br><br>
Alternatively, you can log into your organization through yc-cli and request token with `yc iam create-token` command.
Then put the token into `YC_TOKEN` env variable or fill `yc_token` variable in terraform vars.
10) Run `terraform init` from your module directory
11) Run `terraform plan -var-file=tvars.json -out=tfplan`
12) Run `terraform apply tfplan`

### View logs

**Requirements**
* `yc managed-kubernetes cluster get-credentials --id <cluster-id> --external --context-name lzy` to create context
* `kubectl config use-context lzy` to use context

**Server logs online**
* `kubectl get pods` to find out server pod name (looks like `lzy-service-<id>`)
* `kubectl get logs -f lzy-service-<id>`

**Worker logs online**
* `kubectl get pods` to find out worker pod name (looks like `lzy-worker-<id>`)
* `kubectl exec -it lzy-worker-<id> -- cat /tmp/lzy-log/worker/worker.log`
