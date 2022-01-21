terraform {
  backend "s3" {
    endpoint   = "storage.yandexcloud.net"
    bucket     = "lzy-internal-tf-state"
    region     = "us-east-1"
    key        = "lzy-internal-tf-state/lzy-internal.tfstate"
    access_key = "L0HV_Wr2pjg5pgolyL7x"
    secret_key = "iK1FJihN4nMl04WSK4aPfgnKBsunOaf-MqNS73HT"

    skip_region_validation      = true
    skip_credentials_validation = true
  }
}

module "common" {
  source                     = "../../common"
  installation_name          = "lzy-yc-internal"
  cloud_id                   = "b1gfcpod5hbd1ivs7dav" //cloud-ai
  folder_id                  = "b1gsbp7ts9oepodda5a1" //lzy
  oauth-github-client-id     = "3d7d75d10cb7e4cdc91d"
  oauth-github-client-secret = "19097b7cfce363dcf937527a5fba48ac3763d6af"
  network_id                 = "enps564p8bp649d38qgh" //cloud-ml-dev-nets
  subnet_id                  = "e9brt77u0kenestd9232" //cloud-ml-dev-nets-ru-central1-a
  YC_TOKEN                   = "t1.9euelZqZk4nLl4uSnMuMiYnIjZGUne3rnpWazY-Mms6LjJmeyYrGyJeJjpfl9PdkIFZw-e9jTQfL3fT3JE9TcPnvY00Hyw.P9yahdS1KvEZWB44nfwpbP1VRvWtUwrhz3fXqcw6ytSlSg84NBMYN-nc_NjcadyDDekQwnIaiJMn_TCFgLwPCA"
  s3-separated-per-bucket    = true
  server-image = "lzydock/lzy-server:testing-2"
  servant-image = "lzydock/lzy-servant:testing-2"

  gpu_count = 0
}