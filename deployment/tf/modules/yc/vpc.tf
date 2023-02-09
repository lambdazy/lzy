data "yandex_vpc_network" "custom-net" {
  network_id = var.network_id
}

resource "yandex_vpc_subnet" "custom-subnet" {
  name           = "subnet1"
  zone           = "ru-central1-a"
  network_id     = data.yandex_vpc_network.custom-net.id
  v4_cidr_blocks = ["192.168.10.0/24"]
  route_table_id = yandex_vpc_route_table.rt.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = "nat-gateway"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "rt" {
  name       = "nat-table"
  network_id = data.yandex_vpc_network.custom-net.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_vpc_address" "workflow_public_ip" {
  name = "workflow-dev-address"

  external_ipv4_address {
    zone_id = var.zone
  }
}

resource "yandex_vpc_address" "backoffice_public_ip" {
  name = "backoffice-dev-address"

  external_ipv4_address {
    zone_id = var.zone
  }
}

resource "yandex_vpc_address" "whiteboard_public_ip" {
  name = "whiteboard-dev-address"

  external_ipv4_address {
    zone_id = var.zone
  }
}