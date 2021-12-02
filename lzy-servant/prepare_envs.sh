#!/bin/bash

./create_env.sh "default" "3.9.7" \
    && ./create_env.sh "py37" "3.7.11" \
    && ./create_env.sh "py38" "3.8.12" \
    && ./create_env.sh "py39" "3.9.7" \
#    && ./create_env.sh "py310" "3.10.0"
