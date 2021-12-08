package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;

// TODO (lindvv): deprecate it
public class ProcessEnvironment implements BaseEnvironment {

    @Override
    public LzyProcess runProcess(String... command) throws LzyExecutionException {
        return runProcess(String.join(" ", command), null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException {
        try {
            final Process exec = Runtime.getRuntime().exec(command, envp);
            return new LzyProcess() {
                @Override
                public OutputStream in() {
                    return exec.getOutputStream();
                }

                @Override
                public InputStream out() {
                    return exec.getInputStream();
                }

                @Override
                public InputStream err() {
                    return exec.getErrorStream();
                }

                @Override
                public int waitFor() {
                    try {
                        return exec.waitFor();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void signal(int sigValue) {
                    try {
                        Runtime.getRuntime().exec("kill -" + sigValue + " " + exec.pid());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
