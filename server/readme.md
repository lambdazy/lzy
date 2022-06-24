## Local run [Server](../server)

### Application configuration

module: `server`

[Main class](src/main/java/ai/lzy/server/LzyServer.java):
`LzyServer`

VM options:

```
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32
-Dlzy.server.task.type=local-docker
```

Program arguments:

```
--port 8888
```