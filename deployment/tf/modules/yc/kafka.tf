module "kafka" {
  count = var.enable_kafka ? 1 : 0
  source                       = "./kafka"
  subnet_id                    = yandex_vpc_subnet.custom-subnet.id
  kuber_host                   = yandex_kubernetes_cluster.main.master.0.external_v4_endpoint
  cluster_ca_certificate       = yandex_kubernetes_cluster.main.master.0.cluster_ca_certificate
  cluster_token                = data.yandex_client_config.client.iam_token
}

locals {
  kafka_env_map = var.enable_kafka ? {
    KAFKA_ENABLED                 = "true"
    KAFKA_BOOTSTRAP_SERVERS       = module.kafka[0].bootstrap-servers
    KAFKA_TLS_ENABLED             = "true"
    KAFKA_TLS_TRUSTSTORE_PATH     = "/jks/truststore.jks"
    KAFKA_TLS_TRUSTSTORE_PASSWORD = module.kafka[0].jks-password
    KAFKA_SCRAM_USERNAME          = module.kafka[0].admin-username
    KAFKA_SCRAM_PASSWORD          = module.kafka[0].admin-password
  } : {
    KAFKA_ENABLED                 = "false"
  }
}