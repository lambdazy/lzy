#!/bin/bash

cd /tmp/lzy

export ZYGOTE='{
                 "fuze": "echo 42",
                 "env": {
                    "dockerEnv": {
                    }
                 }
               }'

sbin/run