#!/bin/bash

set -e

LOCAL_V6=$(ip -6 a show dev eth0 | grep inet6 | grep -v fe80 | awk '{printf $2}' | awk -F/ '{printf $1}')
REMOTE_V6=$1
POD_ADDRESS=$2
PODS_CIDR=$3
TUN_NO=$4

echo "Configuration:"
echo "TUN_NO = $TUN_NO"
echo "LOCAL_V6 = $LOCAL_V6"
echo "REMOTE_V6 = $REMOTE_V6"
echo "POD_ADDRESS = $POD_ADDRESS"
echo "PODS_CIDR = $PODS_CIDR"

# TUNNEL
echo "ip -6 tun add tunl$TUN_NO mode ipip6 local $LOCAL_V6 remote $REMOTE_V6 || true"
ip -6 tun add tunl$TUN_NO mode ipip6 local $LOCAL_V6 remote $REMOTE_V6 || true

echo "ip link set dev tunl$TUN_NO up || true"
ip link set dev tunl$TUN_NO up || true

echo "ip a add dev tunl$TUN_NO 100.64.$TUN_NO.2/24 || true"
ip a add dev tunl$TUN_NO 100.64.$TUN_NO.2/24 || true

# ROUTING FROM POD
echo "ip ru del pr 115 || true"
ip ru del pr 115 || true

echo "ip ru del pr 116 || true"
ip ru del pr 116 || true

echo "ip ru del pr 120 || true"
ip ru del pr 120 || true

echo "ip route add default table 120 || true"
ip route add default table 120 || true

echo "ip rule add priority 115 from $POD_ADDRESS to $PODS_CIDR table main"
ip rule add priority 115 from $POD_ADDRESS to $PODS_CIDR table main

echo "ip rule add priority 116 from $POD_ADDRESS to $(echo $LZY_ALLOCATOR_ADDRESS | awk -F: '{printf $1}') table main"
ip rule add priority 116 from $POD_ADDRESS to $(echo $LZY_ALLOCATOR_ADDRESS | awk -F: '{printf $1}') table main

echo "ip rule add priority 120 from $POD_ADDRESS table 120"
ip rule add priority 120 from $POD_ADDRESS table 120

echo "ip route add default via 100.64.$TUN_NO.1 table 120"
ip route add default via 100.64.$TUN_NO.1 table 120

# DNS
cp /etc/resolv.conf resolv-conf-old

{
  echo "nameserver 100.64.$TUN_NO.1"
  echo "options edns0 trust-ad"
  echo "search ru-central1.internal auto.internal"
  cat resolv-conf-old
} > /etc/resolv.conf

echo "new /etc/resolv.conf:"
cat /etc/resolv.conf
