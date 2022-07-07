RED='\033[0;31m'
GREEN='\033[32m'
NC='\033[0m' # No Color

print_red() {
    printf "${RED}%s${NC}\n" "$1"
}

print_green() {
    printf "${GREEN}%s${NC}\n" "$1"
}

println() {
    printf "%s\n" "$1"
}
