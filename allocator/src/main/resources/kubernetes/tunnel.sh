#!/bin/bash


# TUNNEL
ip -6 tun add tunl$TUN_NO mode ipip6 local $LOCAL_V6 remote $REMOTE_v6
ip link set dev tunl%TUN_NO up
ip a add dev tunl$TUN_NO 100.64.$TUN_NO.1/64

# ROUTING FROM POD
ip rule add priority 115 from $POD_ADDRESS to $PODS_CIDR table main
ip rule add priority 120 from $POD_ADDRESS table 120
ip route add default via 100.64.$TUN_NO.1 table 120

# DNS
cp /etc/resolv.conf resolv-conf-old

{
  echo "nameserver 100.64.$TUN_NO.1"
  echo "options edns0 trust-ad"
  echo "search ru-central1.internal auto.internal"
  cat temp-resolv-conf
} > /etc/resolv.conf


