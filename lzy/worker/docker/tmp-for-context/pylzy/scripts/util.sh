RED='\033[0;31m'
GREEN='\033[32m'
NC='\033[0m' # No Color

_f="formatter"
_t="typechecker"
_c="command"
_w="Whole pipeline"

# all functions below work with variables _ex, type, cmd
start() {
    return 0
}

finish() {
     _rc_info "$_w"
}

run() {
    (_ex=$?; type="$1"; shift 1; _isolate_run $@; _upd_rc;)
}

_upd_rc() {
    [ $? -eq 0 ] && [ $_ex -eq 0 ]
}

_isolate_run() {
    cmd="$@"
    println "Calling $type:" "$ $cmd"
    $cmd
    _rc_info "$type" "$cmd"
}

_rc_info() {
    (_ex=$?; type="$1"; [ -v 2 ] && cmd="$2"; _print_cmd_exit; return $_ex;)
}

_print_cmd_exit()  {
    [ $_ex -eq 0 ] && pr=print_green || pr=print_red

    $pr "$type run"
    [ -v cmd ] && $pr "$ $cmd"
    $pr "exited with code: $_ex"
}

print_red() {
    printf "${RED}%s${NC}\n" "$@"
}

print_green() {
    printf "${GREEN}%s${NC}\n" "$@"
}

println() {
    printf "%s\n" "$@"
}
