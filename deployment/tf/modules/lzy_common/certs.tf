resource "kubernetes_secret" "real-certs" {
  count = var.ssl-enabled ? 1 : 0
  metadata {
    name = "certs"
  }

  data = {
    cert = var.ssl-cert
    cert-key = var.ssl-cert-key
  }

  type = "Opaque"
}

resource "kubernetes_secret" "fake-certs" {
  count = var.ssl-enabled ? 0 : 1
  metadata {
    name = "certs"
  }

  data = {
    cert = ""
    cert-key = ""
  }

  type = "Opaque"
}