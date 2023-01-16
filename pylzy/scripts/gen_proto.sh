#!/bin/bash

set -eux

src_dir="$(dirname $0)"
source "$src_dir/util.sh"

# this has to be declared as env variable in mk-python-env.nix
proto_out="lzy/proto"
proto_validation_path="../util/util-grpc/src/main/proto/"
proto_model_path="../model/src/main/proto/"
proto_longrunning_path="../lzy/long-running-api/src/main/proto/"
proto_workflow_path="../lzy-api/src/main/proto/"
proto_whiteboard_path="../whiteboard-api/src/main/proto/"

[ -d "$proto_out" ] || mkdir -p "$proto_out"

pip install -r proto_requirements.txt

# check mypy, it's hack actually but for some reason it's not installed
# ok with nix
python -m mypy_protobuf 1>/dev/null 2>&1\
    || pip install mypy-protobuf

print_green "Generating protobuf, grpclib and mypy proto stubs"
print_green "$PWD"

cd "$proto_validation_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="." \
                    '{}' +
cd "$OLDPWD"

cd "$proto_model_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . -I "$OLDPWD/$proto_validation_path" \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="." \
                    '{}' +
cd "$OLDPWD"

cd "$proto_longrunning_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="." \
                    '{}' +
cd "$OLDPWD"

cd "$proto_workflow_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . -I "$OLDPWD/$proto_longrunning_path" -I "$OLDPWD/$proto_model_path" \
       -I "$OLDPWD/$proto_validation_path" \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="." \
                    '{}' +
cd "$OLDPWD"

cd "$proto_whiteboard_path"
find . -iname "*.proto" -type f \
       -exec python -m grpc_tools.protoc -I . -I "$OLDPWD/$proto_model_path" -I "$OLDPWD/$proto_validation_path" \
                    --python_out="$OLDPWD" \
                    --mypy_out="$OLDPWD" \
                    --grpc_python_out="$OLDPWD" \
                    --proto_path="." \
                    '{}' +
cd "$OLDPWD"

find "ai" -type d \
        -exec touch '{}/__init__.py' \;

print_green "Generated next proto stubs:"
println "$proto_out"
find "$proto_out" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
