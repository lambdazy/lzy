resource "kubernetes_manifest" "lzy_backoffice" {
  manifest = {
    "apiVersion" = "v1"
    "kind"       = "Service"
    "metadata" = {
      "annotations" = {
        "yandex.cloud/yandex-only" = "true"
      }
      "name"      = "lzy-backoffice-service"
      "namespace" = "default"
    }
    "spec" = {
      "ports" = [
        {
          name       = "backend"
          port       = 8080
          targetPort = null
        },
        {
          name       = "frontend"
          port       = 80
          targetPort = null
        },
      ]
      "selector" = {
        "app" = "lzy-backoffice"
      }
      "type" = "LoadBalancer"
      "ipFamilies" = [
        "IPv6",
        "IPv4"
      ]
      "ipFamilyPolicy"        = "PreferDualStack"
      "externalTrafficPolicy" = "Local"
    }
  }
}

resource "kubernetes_manifest" "lzy_kharon" {
  manifest = {
    "apiVersion" = "v1"
    "kind"       = "Service"
    "metadata" = {
      "annotations" = {
        "yandex.cloud/yandex-only" = "true"
      }
      "name"      = "lzy-kharon-service"
      "namespace" = "default"
    }
    "spec" = {
      "ports" = [
        {
          port       = 8899
          targetPort = null
        },
      ]
      "selector" = {
        "app" = "lzy-kharon"
      }
      "type" = "LoadBalancer"
      "ipFamilies" = [
        "IPv6",
        "IPv4"
      ]
      "ipFamilyPolicy"        = "PreferDualStack"
      "externalTrafficPolicy" = "Local"
    }
  }

}