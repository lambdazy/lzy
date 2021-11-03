package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;

public interface Connector {
    Process exec(String command) throws IOException, InterruptedException;
}
