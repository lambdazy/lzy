#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

set -ux

source ./util.sh

# this has to be declarared as env variable in mk-python-env.nix
# proto_out="lzy/proto"
# proto_path="../model/src/main/proto/"

[ ! -d "$proto_out" ] && mkdir -p "$proto_out/bet"

# check mypy, it's hack actually but for some reason it's not installed
# ok with nix
python -m mypy_protobuf 1>/dev/null 2>&1
[ $? -ne 0 ] && pip install mypy-protobuf

print_green "Generating protobuf, grpclib and mypy proto stubs"

cd "$proto_path" && \
    find . -iname "*.proto" -type f \
        -print \
        -exec python -m grpc_tools.protoc \
                     -I . \
                     --python_out="$proto_out" \
                     --mypy_out="$proto_out" \
                     --grpclib_python_out="$proto_out" \
                     --proto_path="$proto_path" \
                     '{}' +

cd "$proto_out" && \
   mv lzy/proto/* ./ && \
   rmdir -p "lzy/proto"

out="$proto_out/bet"
print_green "Generating betterproto dataclasses stubs in $out"

cd "$proto_path" && \
    find . \
        -iname "*.proto" -type f \
        ! -name "lzy-graph-executor.proto" \
        ! -name "lzy-server.proto" \
        ! -name "lzy-kharon.proto" \
        -exec python -m grpc_tools.protoc \
                     -I . \
                     --python_betterproto_out="$out" \
                     '{}' +

cd "$out" && \
   mv lzy/proto/* ./ && \
   rm "lzy/__init__.py" && \
   rmdir -p "lzy/proto"

print_green "Generated next proto stubs:"
println "$proto_out"
find "$proto_out" -type f \( -iname "*.py" -o -iname "*.pyi" \) -print
