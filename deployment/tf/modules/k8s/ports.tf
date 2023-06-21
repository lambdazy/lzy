locals {
  allocator-metrics-port       = 17080
  channel-manager-metrics-port = 17080
  iam-metrics-port             = 17080
  graph-executor-metrics-port  = 17080
  lzy-service-metrics-port     = 17080
  scheduler-metrics-port       = 17080
  site-metrics-port            = 17080
  storage-metrics-port         = 17080
  whiteboard-metrics-port      = 17080

  allocator-port               = 10239
  allocator-http-port          = 8082
  whiteboard-port              = 8122
  storage-port                 = 8122
  backoffice-frontend-port     = 80
  backoffice-frontend-tls-port = 443
  backoffice-backend-port      = 8080
  backoffice-backend-tls-port  = 8443
  scheduler-port               = 8765
  lzy-service-port             = 8899
  iam-port                     = 8443
  graph-port                   = 8122
  channel-manager-port         = 8122

  fluent-bit-port              = 24224
}