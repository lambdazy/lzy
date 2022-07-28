RED='\033[0;31m'
GREEN='\033[32m'
NC='\033[0m' # No Color

_f="formatter"
_t="typechecker"

# all functions below work with variables _ex, type, cmd
print_cmd_exit() {
    [ "$_ex" -eq 0 ] && pr=print_green || pr=print_red

    $pr "$type run"
    [ -v cmd ] && $pr "$ $cmd"
    $pr "exited with code: $_ex"
}

print_pipeline_exit() {
    (_ex=$rc; type="Whole pipeline"; print_cmd_exit;)
}

run() {
    (type="$1"; cmd="$2"; _isolate_run; upd_rc;)
}

_isolate_run() {
    println "Calling $type:" "$ $cmd"
    $cmd_name
    _ex=$?
    print_cmd_exit
}

upd_rc() {
    [ $rc -eq 0 ] && [ $_ex -eq 0 ]
    rc=$?
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
