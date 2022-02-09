package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

// TODO 87917: deprecate it
public class ProcessEnvironment implements BaseEnvironment {

    @Override
    public LzyProcess runProcess(String... command)
        throws LzyExecutionException {
        try {
            final Process exec = Runtime.getRuntime().exec(command);
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
