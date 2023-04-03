#!/bin/bash

set -ex

TUNNEL=$(ip link | grep tunl | awk -F': ' '{print $2}')

if [[ -z $TUNNEL ]]; then
  echo "No tunnel found on the host. Host has following network devices:"
  ip link
  exit 1
fi

# DELETE RULES
ip ru del pr 115 || true
ip ru del pr 116 || true
ip ru del pr 120 || true

# DELETE ROUTE TABLE
ip route flush table 120 || true

# DELETE TUNNEL
ip link set dev $TUNNEL down || true
ip link delete $TUNNEL || true

# RESTORE DNS
if [[ -f resolve-conf-old ]]; then
  cp resolv-conf-old /etc/resolv.conf

  echo "Restored /etc/resolv.conf:"
  cat /etc/resolv.conf
else
  echo "Old resolv.conf isn't found"
  exit 1
fi

