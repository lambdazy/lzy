#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

set -ux

# this vars have to be declarared as env variables in mk-python-env.nix
# proto_out="lzy/proto"
# proto_path="../model/src/main/proto/"

[ ! -d "$proto_out" ] && mkdir -p "$proto_out"


cd "$proto_path"
python -m grpc_tools.protoc \
    -I . \
    --python_betterproto_out="$OLDPWD/$proto_out" \
    --proto_path=. \
    $(find ai/lzy/priv -iname "*.proto" -type f)
cd "$OLDPWD"


echo "Generated next proto stubs:"
find "$proto_out" -type f -iname "*.py"
