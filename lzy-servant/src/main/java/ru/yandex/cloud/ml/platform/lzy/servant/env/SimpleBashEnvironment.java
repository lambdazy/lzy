package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public class SimpleBashEnvironment implements Environment {

    @Override
    public void prepare() throws EnvironmentInstallationException {
    }

    @Override
    public Process exec(String command) throws LzyExecutionException {
        try {
            return Runtime.getRuntime().exec(new String[] {"bash", "-c", command});
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }
}
