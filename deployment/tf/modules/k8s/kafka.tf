module "kafka" {
  count = var.enable_kafka ? 1 : 0
  source                       = "./kafka"
  subnet_id                    = var.custom-subnet-id
}

locals {
  kafka_env_map = var.enable_kafka ? {
    KAFKA_ENABLED                 = "true"
    KAFKA_TLS_ENABLED             = "true"
    KAFKA_TLS_TRUSTSTORE_PATH     = "/truststore/truststore.p12"
    KAFKA_TLS_TRUSTSTORE_PASSWORD = module.kafka[0].truststore-password
    KAFKA_TLS_KEYSTORE_PATH       = "/keystore/keystore.p12"
    KAFKA_TLS_KEYSTORE_PASSWORD   = module.kafka[0].keystore-password
  } : {
    KAFKA_ENABLED                 = "false"
  }
}