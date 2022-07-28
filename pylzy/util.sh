RED='\033[0;31m'
GREEN='\033[32m'
NC='\033[0m' # No Color

_f="formatter"
_t="typechecker"

# all functions below work with variables _ex, type, cmd
print_cmd_exit() {
    (_ex=$?; _print_cmd_exit;)
}

_print_cmd_exit()  {
    [ $_ex -eq 0 ] && pr=print_green || pr=print_red

    $pr "$type run"
    [ -v cmd ] && $pr "$ $cmd"
    $pr "exited with code: $_ex"
}

print_pipeline_exit() {
    (_ex=$rc; type="Whole pipeline"; _print_cmd_exit;)
}

start() {
    return 0
}

finish() {
    (rc=$?; print_pipeline_exit; exit $rc;)
     exit $?
}

run() {
    (rc=$?; type="$1"; cmd="$2"; _isolate_run; upd_rc;)
}

_isolate_run() {
    println "Calling $type:" "$ $cmd"
    $cmd
    print_cmd_exit && return $?
}

upd_rc() {
    [ $rc -eq 0 ] && [ $? -eq 0 ]
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
