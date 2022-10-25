#!/bin/bash

./scripts/gen_proto.sh
pip install tox
tox
