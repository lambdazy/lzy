#!/usr/bin/env nix-shell
#! nix-shell build.nix -A dev -i bash

set -eux

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

# this has to be declared as env variable in mk-python-env.nix
# proto_out="lzy/proto"
# proto_path="../model/src/main/proto/"

[ -d "$proto_out" ] || mkdir -p "$proto_out"

# check mypy, it's hack actually but for some reason it's not installed
# ok with nix
python -m mypy_protobuf 1>/dev/null 2>&1\
    || pip install mypy-protobuf

print_green "Generating protobuf, grpclib and mypy proto stubs"

cd "$proto_workflow_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpclib_python_out="$OLDPWD" \
                    --proto_path="$proto_workflow_path" \
                    '{}' +
cd "$OLDPWD"

cd "$proto_model_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . -I "$proto_workflow_path" \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpclib_python_out="$OLDPWD" \
                    --proto_path="$proto_model_path" \
                    '{}' +

cd "$OLDPWD"

find "ai" -type d \
        -exec touch '{}/__init__.py' \;

print_green "Generated next proto stubs:"
println "$proto_out"
find "$proto_out" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
