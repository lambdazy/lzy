package ru.yandex.cloud.ml.platform.lzy.servant.env;

import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

import java.io.IOException;

public class SimpleBashEnvironment implements Environment {
    @Override
    public Process exec(String command) throws LzyExecutionException {
        try {
            return Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }
}
