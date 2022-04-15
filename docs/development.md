## Development

### Before local run

* For macOS: install [macFuse](https://osxfuse.github.io)

* For arch linux:
```
pacman -Sy fuse2 inetutils
```

### Local run

1. Run [Server](lzy-server/readme.md)
2. Run [Kharon](lzy-kharon/readme.md)
3. Run [Terminal](lzy-servant/readme.md)
4. Now ʎzy FS should be available at path `/tmp/lzy`
---
**For python API:**

5. Install [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
6. `cd lzy-servant/ && ./prepare_envs.sh ../lzy-python` (conda envs setup)

### FAQ

* ```Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found```: Java > 11 is required