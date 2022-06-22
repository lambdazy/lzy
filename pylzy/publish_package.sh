#!/bin/bash
# ! this script should be ran from servant directory

clean() {
    echo "Clean up"
    rm lzy/lzy-servant.jar 
    rm -rf *.egg-info dist/ build/
}

build() {
    dev=$1
    echo "Building pylzy package"
    python -m pip install --upgrade build twine
    # TODO: pass jar path to script as parameter?
    cp ../servant/target/servant-1.0-SNAPSHOT.jar lzy/lzy-servant.jar

    # instead of
    # python -m build # which builds both wheel dist and source dist
    # 
    # build manually because I couldn't find the way to pass arguments to
    # nightly package pylzy-nightly:
    python setup.py sdist $dev bdist_wheel $dev # pass --dev flag if needed

}

publish() {
    echo "Calling publish commands"
    # this command will ask tokens,
    # **token has to be provided by person or bot who runs the script**
    python -m twine upload dist/* --verbose
}

usage() {
    echo "Usage: $0 [-b] [-d] [-s] [-c]" 1>&2
    echo "-d to build nightly package" 1>&2
    echo "-b to run **only** building stage" 1>&2
    echo "-c to run **only** cleaning stage" 1>&2
    echo "-s to skip cleaning stage" 1>&2
}


while getopts "hbdsc" arg; do
    case $arg in
        b)
            echo "Build only"
            build
            exit 0
            ;;
        c)
            echo "Clean only"
            clean
            exit 0
            ;;
        d)
            dev='--dev'
            ;;
        s)
            skip_clean=1
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "wrong option $arg"
            usage
            exit 1
            ;;
    esac
done

build $dev
publish
[ ! -v skip_clean ] || [ $skip_clean -ne 1 ] && clean
