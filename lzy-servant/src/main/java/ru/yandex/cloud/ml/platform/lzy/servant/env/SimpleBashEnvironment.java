package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;

public class SimpleBashEnvironment implements Environment {

    @Override
    public Process exec(String command) throws LzyExecutionException {
        try {
            return Runtime.getRuntime().exec(new String[] {"bash", "-c", command});
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }
}
