module "kafka" {
  source = "./kafka"
  subnet_id = yandex_vpc_subnet.custom-subnet.id
}