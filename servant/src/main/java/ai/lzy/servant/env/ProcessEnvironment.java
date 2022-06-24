package ai.lzy.servant.env;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProcessEnvironment implements BaseEnvironment {

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(String.join(" ", command), null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
