## Development

### Before local run

-   For macOS: install [macFuse](https://osxfuse.github.io)

-   For arch linux:

<!-- -->

    pacman -Sy fuse2 inetutils

### Prepare env for python integration tests

1.  Install conda
2.  Prepare conda env:
    `conda create --name "py39" "python=3.9.7" && conda activate py39 && pip install -r lzy-servant/docker/requirements.txt && cd lzy-python && pip install -r requirements.txt && python setup.py install && cd ..`

### Local run

1.  Run [Server](lzy-server/readme.md)
2.  Run [Kharon](lzy-kharon/readme.md)
3.  Run [Terminal](lzy-servant/readme.md)
4.  Now ʎzy FS should be available at path `/tmp/lzy`

------------------------------------------------------------------------

**For python API:**

5.  Install
    [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
6.  `cd lzy-servant/ && ./prepare_envs.sh ../lzy-python` (conda envs
    setup)

### FAQ

-   `Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found`:
    Java > 17 is required

------------------------------------------------------------------------

## (New) Development with nix

Firstly [install nix](https://nixos.org/download.html) via:

``` sh
sh <(curl -L https://nixos.org/nix/install) --daemon
```

### Run options

Call one of scripts in `lzy-python` for action:

-   `./run_tests.sh` -- run all available python unit tests.
-   `./build.nix` or `./buind.nix --dev` -- build pylzy package or
    pylzy-nightly (if run with `--dev`) and place it in `dist/` dir.
-   `./publish.sh` -- try to publish package placed in `dist`.
    `TWINE_PASSWORD` env variable should be provided to successfuly
    publish package, otherwise publication will be paused later and
    password will be required.
-   `./clean.sh` -- clean **all** build artifacts.

### Python env

Currently there are several environments: `shell`, `shell-default` and
`shell-lzy` (see `lzy-python/build.nix` for details).

If you want to open one of them:

``` sh
cd lzy-python/

# use default shell if you don't know which to open
nix-shell build.nix -A shell 
```
