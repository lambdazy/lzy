#!/bin/bash -e

if [[ @# -lt 2 ]]; then
  echo "Usage: $0 <username> <password>"
  exit 1
fi

echo "Publishing package"

export TWINE_USERNAME=$1
export TWINE_PASSWORD=$2

pip install twine -U
python -m twine upload dist/* --verbose
