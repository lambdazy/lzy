module "allocator" {
  source = "../allocator"
  allocator-image = "cr.cloud-preprod.yandex.net/crt565akmosika2bc91r/allocator:preprod"
  iam-image = "cr.cloud-preprod.yandex.net/crt565akmosika2bc91r/iam:preprod"
  cloud_id = "aoec9cvb8rh7s06sgki2"
  folder_id = "aoeo1dfromevbs79kijh"
  network_id = "c6411a5sdbcq67eacggg"
  subnet-id = "buc7cobctuqm3rnnjsoi"
  subnet-folder = "aoef893gt5c5ivo37deq"
  yc-token = "<your-yc-token>",
  yc-endpoint = "api.cloud-preprod.yandex.net:443"
  user-clusters = ["c49jng4f4rikm44i85lm"]
}

terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
  }

  backend "s3" {
    endpoint = "storage.cloud-preprod.yandex.net"
    bucket   = "lzy-internal-tf-state"
    region   = "us-east-1"
    key      = "lzy-tf-state"

    access_key = "YCBFBDew7uEhyxTQZM7YbNRAx"
    secret_key = "<your-secret-key>"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
}
