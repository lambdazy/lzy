terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "0.68.0"
    }
    random = {
      version = ">=3.0.1"
    }
    kubernetes = {
      version = "=2.11.0"
    }
  }
}
