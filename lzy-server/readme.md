## Local run [Server](../lzy-server)

### Application configuration

module: `lzy-server`

[Main class](src/main/java/ru/yandex/cloud/ml/platform/lzy/server/LzyServer.java):
`ru.yandex.cloud.ml.platform.lzy.server.LzyServer`

VM options:
```
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32
-Dlzy.server.task.type=local-docker
```

Program arguments:
```
--port 8888
```