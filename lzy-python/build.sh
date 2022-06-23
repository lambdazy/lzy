#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell -i bash

./gen_proto.sh

echo "Building pylzy package"
# TODO: pass jar path to script as parameter?
cp ../lzy-servant/target/lzy-servant-1.0-SNAPSHOT.jar lzy/lzy-servant.jar
[ $? -ne 0 ] && echo "Failed to copy lzy-servant jar" && exit 1

# instead of
# python -m build
# which builds both wheel dist and source dist
# 
# build by directly executing setup.py
# I couldn't find the better way to pass arguments to build
python setup.py sdist "$@" bdist_wheel "$@"  # pass --dev flag if needed
