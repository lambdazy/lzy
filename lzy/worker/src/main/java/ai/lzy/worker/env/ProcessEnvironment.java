package ai.lzy.worker.env;

import ai.lzy.worker.StreamQueue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessEnvironment implements BaseEnvironment {

    @Override
    public void install(StreamQueue out, StreamQueue err) {}

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(String.join(" ", command), null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        if (envp != null) {
            envp = inheritEnvp(envp);
        }
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

    private String[] inheritEnvp(String[] envp) {
        List<String> systemEnvs = System.getenv().entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());
        systemEnvs.addAll(List.of(envp));
        return systemEnvs.toArray(String[]::new);

    }
}
