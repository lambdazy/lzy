terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.82.0"
    }
  }
  backend "s3" {
    endpoint = "storage.yandexcloud.net"
    bucket   = "lzy-tfstate" //example bucket for tfstate
    region   = "us-east-1"
    key      = "lzy-tfstate/lzy-tfstate" //example key for tfstate

    skip_region_validation      = true
    skip_credentials_validation = true
    #you must place your s3 credentials to env variables or aws config file
  }
}

locals {
  cloud_id   = "<your cloud id>"
  folder_id  = "<your folder id>"
  zone       = "ru-central1-a" #example zone
  network_id = "<your network id>"
}

provider "yandex" {
  cloud_id    = local.cloud_id
  folder_id   = local.folder_id
  zone        = local.zone
  token       = var.yc_token
  max_retries = 10
}

module "v2" {
#  source                    = "github.com/lambdazy/lzy//deployment/tf/modules/v2?ref=master"
  source                    = "../v2"
  installation_name         = "lzy-dev-public" //you can change it
  folder_id                 = local.folder_id
  network_id                = local.network_id
  yc-endpoint               = "api.cloud.yandex.net:443"
  user-clusters             = []
  max-servants-per-workflow = "2"
  worker-limits-by-labels = {
    "s" = 2
  }

  oauth-github-client-id     = var.oauth_github_client_id
  oauth-github-client-secret = var.oauth_github_client_secret

  channel-manager-image     = "lzydock/channel-manager:${var.docker_images_tag}"
  allocator-image           = "lzydock/allocator:${var.docker_images_tag}"
  iam-image                 = "lzydock/iam:${var.docker_images_tag}"
  scheduler-image           = "lzydock/scheduler:${var.docker_images_tag}"
  servant-image             = "lzydock/worker:${var.docker_worker_image_tag}"
  graph-image               = "lzydock/graph-executor:${var.docker_images_tag}"
  lzy-service-image         = "lzydock/lzy-service:${var.docker_images_tag}"
  portal_image              = "lzydock/portal:${var.docker_images_tag}"
  storage-image             = "lzydock/storage:${var.docker_images_tag}"
  whiteboard-image          = "lzydock/whiteboard:${var.docker_images_tag}"
  backoffice-backend-image  = "lzydock/site-backend:${var.docker_images_tag}"
  backoffice-frontend-image = "lzydock/site-frontend:${var.docker_images_tag}"
  unified-agent-image       = "lzydock/unified_agent:${var.docker_unified_agent_tag}"
}
