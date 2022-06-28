#!/usr/bin/env nix-shell
#! nix-shell build.nix -A shell-lint -i bash

source ./util.sh

print_cmd_exit() {
    _ex=$1
    msg="$2 exited with code: $_ex"
    [ $_ex -eq 0 ] && print_green "$msg" || print_red "$msg"
}

run() {
    cmd_name=$1
    type=$2

    println "Calling $type: $cmd_name"

    $cmd_name .
    _ex=$?
    print_cmd_exit $_ex $cmd_name

    [ $rc -eq 0 ] && [ $_ex -eq 0 ]
    rc=$?
}


_f="formatter"
_t="typechecker"

rc=0

run black   $_f
run isort   $_f
run mypy    $_t
run pyright $_t

print_cmd_exit $rc "Whole pipeline"
[ $rc -eq 0 ]
exit $?
