#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash


echo "Calling mypy typechecking"
mypy .
mypy_exit=$?
echo "Mypy exited with code $mypy_exit"

echo "Calling pyright typechecking"
pyright .
pyright_exit=$?
echo "Calling pyright exited with code $pyright_exit"

[ $mypy_exit -ne 0 ] && exit $mypy_exit
[ $pyright_exit -ne 0 ] && exit $pyright_exit
exit 0
