## Development

### Before local run

* For macOS: install [macFuse](https://osxfuse.github.io)

* For arch linux:
```
pacman -Sy fuse2 inetutils
```

### Prepare env for python integration tests

1. Install conda
2. Prepare conda env: `conda create --name "py39" "python=3.9.7" && conda activate py39 && pip install -r servant/docker/requirements.txt && cd python && pip install -r requirements.txt && python setup.py install && cd ..`

### Local run

1. Run [Server](server/readme.md)
2. Run [Kharon](kharon/readme.md)
3. Run [Terminal](servant/readme.md)
4. Now ʎzy FS should be available at path `/tmp/lzy`
---
**For python API:**

5. Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
6. `cd servant/ && ./prepare_envs.sh ../python` (conda envs setup)

### FAQ

* ```Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found```: Java > 17 is required