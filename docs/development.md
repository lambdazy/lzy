## Development

### Before local run

-   For macOS: install [macFuse](https://osxfuse.github.io)

-   For arch linux:

``` sh
    pacman -Sy fuse2 inetutils
```

### Prepare env for python integration tests

1.  Install conda
2.  Prepare conda env:
    `conda create --name "py39" "python=3.9.7" && conda activate py39 && pip install -r worker/docker/requirements.txt && cd python && pip install -r requirements.txt && python setup.py install && cd ..`

### Local run

#### Intellij IDEA

Just run `RunAllServices` configuration.

#### Terminal
Install jdk >= 17 and maven for your OS.

Then run:
``` bash
    mvn install -DskipTests
    cd test && mvn test -Dtest=RunAll
```

### FAQ

-   `Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found`:
    Java > 17 is required

