![](https://github.com/lambda-zy/lzy/actions/workflows/pull-request-workflow.yaml/badge.svg)

# ʎzy

λzy is a system for the distributed execution of an arbitrary code and storage of the obtained results.

We believe that the goals of this system are:
- Transparent scaling of code that is not generally intended for distributed execution
- Run ML training and inference tasks in one computing environment, effectively balancing the load between these circuits.
- Provide an ability to combine local and distributed components in one task.
- Allow an ML specialist to implement an arbitrary configuration of the computing environment (MR, main-secondary, rings/trees, etc.)

The system is based on the following principle: a computing cluster is represented as one large UNIX machine. Calculations communicate with each other using shared files, pipes, and other familiar machinery. The user controls this large UNIX machine either from his local laptop, where the system crawls through a partition in the file system.

## Before local run

* For macOS: install [macFuse](https://osxfuse.github.io)

* For arch linux:
```
pacman -Sy fuse2 inetutils
```

## Local run

1. Run `ru.yandex.cloud.ml.platform.lzy.server.LzyServer`
2. Run `ru.yandex.cloud.ml.platform.lzy.kharon.LzyKharon`
3. Run `ru.yandex.cloud.ml.platform.lzy.servant.BashApi` with arguments:
   1. `--lzy-address localhost:8899` (server address)
   2. `--lzy-mount /tmp/lzy` (path for ʎzy FS)
   3. `--host localhost` (host for terminal)
   4. `--internal-host host.docker.internal` (docker support; **only for macOS**)
   5. `terminal` (command name)
4. Now ʎzy FS should be available at path `/tmp/lzy`

## FAQ

* ```Exception in thread "main" java.lang.UnsatisfiedLinkError: dlopen(libfuse.dylib, 9): image not found```: Java > 11 is required
