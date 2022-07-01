#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

set -ux

source ./util.sh

# this has to be declarared as env variable in mk-python-env.nix
# proto_out="lzy/proto"
# proto_path="../model/src/main/proto/"

[ ! -d "$proto_out" ] && mkdir -p "$proto_out"

# check mypy, it's hack actually but for some reason it's not installed
# ok with nix
python -m mypy_protobuf 1>/dev/null 2>&1
[ $? -ne 0 ] && pip install mypy-protobuf

print_green "Generating betterproto dataclasses stubs"
cd "$proto_path"
python -m grpc_tools.protoc \
       -I . \
       --python_out="$OLDPWD/$proto_out" \
       --mypy_out="$OLDPWD/$proto_out" \
       --grpclib_python_out="$OLDPWD/$proto_out" \
       --proto_path="$proto_path" \
       $(find . -iname "*.proto" -type f)
cd "$OLDPWD"

print_green "Generating betterproto dataclasses stubs"
cd "$proto_path"
python -m grpc_tools.protoc \
       -I . \
       --python_betterproto_out="$OLDPWD/$proto_out" \
       $(find . \
              -iname "*.proto" -type f \
              ! -name "lzy-graph-executor.proto" \
              ! -name "lzy-server.proto" \
              ! -name "lzy-kharon.proto" )
cd "$OLDPWD"

cd "$proto_out/lzy/proto"
find . -maxdepth 1 -exec mv {} ../.. \;
rmdir lzy/proto && rmdir lzy/
cd "$OLDPWD"

print_green "Generated next proto stubs:"
find "$proto_out" -type f -iname "*.py"
