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
     _print_ce "$_w"
}

run() {
    (_ex=$?; type="$1"; cmd="$2"; _isolate_run; upd_rc;)
}

upd_rc() {
    [ $? -eq 0 ] && [ $_ex -eq 0 ]
}

_isolate_run() {
    println "Calling $type:" "$ $cmd"
    $cmd
    _print_ce "$type"
}

_print_ce() {
    (_ex=$?; type="$1"; _print_cmd_exit; exit $_ex;)
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
