package ru.yandex.cloud.ml.platform.lzy.servant.env;

import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;

public interface Environment {
    Process exec(String command) throws LzyExecutionException;
}
