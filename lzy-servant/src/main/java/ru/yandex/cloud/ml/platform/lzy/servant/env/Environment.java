package ru.yandex.cloud.ml.platform.lzy.servant.env;

import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public interface Environment {
    void prepare() throws EnvironmentInstallationException;
    Process exec(String command) throws LzyExecutionException;
    Process exec(String command, String[] envp) throws LzyExecutionException;
}
