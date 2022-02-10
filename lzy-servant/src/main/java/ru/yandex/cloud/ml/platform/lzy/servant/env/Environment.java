package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.InputStream;
import java.io.OutputStream;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public interface Environment extends AutoCloseable {
    void prepare() throws EnvironmentInstallationException;
    LzyProcess runProcess(String... command) throws LzyExecutionException;
    LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException;
    default LzyProcess runProcess(String command, String[] envp) throws LzyExecutionException {
        return runProcess(new String[]{command}, envp);
    }

    interface LzyProcess {
        OutputStream in();
        InputStream out();
        InputStream err();
        int waitFor();
        void signal(int sigValue);
    }
}
