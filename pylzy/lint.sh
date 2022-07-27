#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash

set -ux

source ./util.sh

print_cmd_exit() {
    _ex=$1
    msg="$2 exited with code: $_ex"
    [ $_ex -eq 0 ] && print_green "$msg" || print_red "$msg"
}

run() {
    type=$1
    cmd_name=$2

    println "Calling $type: $cmd_name"

    $cmd_name

    _ex=$?
    print_cmd_exit $_ex $cmd_name

    [ $rc -eq 0 ] && [ $_ex -eq 0 ]
    rc=$?
}


_f="formatter"
_t="typechecker"

rc=0

run $_f "black lzy/ tests/ examples/"
run $_f "isort lzy/ tests/ examples/"
run $_t "mypy --install-types --non-interactive --check-untyped-defs --show-error-codes --pretty -p ai.lzy.v1 -p lzy"
# run $_t pyright

print_cmd_exit $rc "Whole pipeline"

exit $rc
