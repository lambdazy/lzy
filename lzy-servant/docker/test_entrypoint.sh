#!/bin/bash

export LOGS_APPENDER="${LOGS_APPENDER:-LogFile}"

eval "$(conda shell.bash hook)"

conda activate default

lzy-terminal "$@"
