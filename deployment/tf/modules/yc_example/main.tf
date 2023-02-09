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

module "yc" {
  #  source                    = "github.com/lambdazy/lzy//deployment/tf/modules/yc?ref=master"
  source                    = "../yc"
  installation_name         = "lzy-dev-public" //you can change it
  folder_id                 = local.folder_id
  network_id                = local.network_id
  yc-endpoint               = "api.cloud.yandex.net:443"

  oauth-github-client-id     = var.oauth_github_client_id
  oauth-github-client-secret = var.oauth_github_client_secret

  channel-manager-image     = "lzydock/channel-manager:${var.docker_channel_manager_image_tag}"
  allocator-image           = "lzydock/allocator:${var.docker_allocator_image_tag}"
  iam-image                 = "lzydock/iam:${var.docker_iam_image_tag}"
  scheduler-image           = "lzydock/scheduler:${var.docker_scheduler_image_tag}"
  servant-image             = "lzydock/worker:${var.docker_worker_image_tag}"
  graph-image               = "lzydock/graph-executor:${var.docker_graph_executor_image_tag}"
  lzy-service-image         = "lzydock/lzy-service:${var.docker_lzy_service_image_tag}"
  portal_image              = "lzydock/portal:${var.docker_portal_image_tag}"
  storage-image             = "lzydock/storage:${var.docker_storage_image_tag}"
  whiteboard-image          = "lzydock/whiteboard:${var.docker_whiteboard_image_tag}"
  backoffice-backend-image  = "lzydock/site:${var.docker_backend_image_tag}"
  backoffice-frontend-image = "lzydock/site-frontend:${var.docker_frontend_image_tag}"
  unified-agent-image       = "lzydock/unified_agent:${var.docker_unified_agent_image_tag}"

  workers_nodegroups_definition = {
    "s" = {
      kind        = "CPU"
      platform_id = "standard-v2"
      resource_spec = {
        cores  = 4
        memory = 32
      }
      scale_policy = {
        fixed        = false
        initial_size = 1
        min_size = 0
        max_size = 2
      }
    }
    "l" = {
      kind        = "GPU"
      platform_id = "gpu-standard-v2"
      resource_spec = {
        memory = 48
        cores  = 8
        gpus   = 1
      }
      scale_policy = {
        fixed        = true
        initial_size = 1
      }
    }
  }
}
