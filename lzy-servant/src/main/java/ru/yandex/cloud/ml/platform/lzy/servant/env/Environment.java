package ru.yandex.cloud.ml.platform.lzy.servant.env;

import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public interface Environment {
    Process exec(String command) throws EnvironmentInstallationException, LzyExecutionException;
    Process exec(String command, String[] envp) throws EnvironmentInstallationException, LzyExecutionException;
}
