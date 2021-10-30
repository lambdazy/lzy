package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;

public interface Connector {
    int execAndLog(String command) throws IOException, InterruptedException;
    Process exec(String command) throws IOException;
}
