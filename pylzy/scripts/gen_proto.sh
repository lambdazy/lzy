#!/bin/bash

set -eux

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

proto_workflow_path="$(pwd)/../workflow-api/src/main/proto/"
proto_out="$(pwd)/ai/lzy/v1"

# this has to be declared as env variable in mk-python-env.nix
# proto_out="lzy/proto"
# proto_path="../model/src/main/proto/"

[ -d "$proto_out" ] || mkdir -p "$proto_out"

pip install mypy-protobuf grpcio-tools

print_green "Generating protobuf, grpclib and mypy proto stubs"

cd "$proto_workflow_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="$proto_workflow_path" \
                    '{}' +
cd "$OLDPWD"

find "ai" -type d \
        -exec touch '{}/__init__.py' \;

print_green "Generated next proto stubs:"
println "$proto_out"
find "$proto_out" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
