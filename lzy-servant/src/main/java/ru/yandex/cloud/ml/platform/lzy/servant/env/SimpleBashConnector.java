package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;

public class SimpleBashConnector implements Connector {
    @Override
    public Process exec(String command) throws IOException {
        return Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
    }
}
