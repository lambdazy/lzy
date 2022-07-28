RED='\033[0;31m'
GREEN='\033[32m'
NC='\033[0m' # No Color

_f="formatter"
_t="typechecker"

print_cmd_exit() {
    _ex=$1
    msg="$2 exited with code: $_ex"
    [ $_ex -eq 0 ] && print_green "$msg" || print_red "$msg"
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
    printf "${RED}%s${NC}\n" "$1"
}

print_green() {
    printf "${GREEN}%s${NC}\n" "$1"
}

println() {
    printf "%s\n" "$1"
}
