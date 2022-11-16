## Local run [Terminal](../worker)

### Application configuration

module: `servant`

[Main class](src/main/java/ai/lzy/worker/BashApi.java):
`ai.lzy.servant.BashApi`

VM options:
```
-Djava.library.path=/usr/local/lib
-Djava.util.concurrent.ForkJoinPool.common.parallelism=32 
```

Program arguments:
* `--lzy-address localhost:8899` (server address)
* `--lzy-mount /tmp/lzy` (path for ʎzy FS)
* `--private-key /Users/<username>/.ssh/private.pem`
* `--host localhost`
* `--port 9990`
* `terminal` (command name)

Environment variables:
```
LOG_FILE="terminal"
```

### Additionally

* Create folder for logs:
  ```
  mkdir -p /var/log/servant
  chown $USER servant
  ```