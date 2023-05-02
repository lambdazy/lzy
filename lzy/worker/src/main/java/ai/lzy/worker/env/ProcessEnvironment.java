package ai.lzy.worker.env;

import ai.lzy.worker.StreamQueue;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessEnvironment extends BaseEnvironment {
    private List<String> env = new ArrayList<>();

    public ProcessEnvironment() {
        super();
    }

    public ProcessEnvironment withEnv(Map<String, String> env) {
        this.env = env.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .toList();
        return this;
    }

    @Override
    public void install(StreamQueue.LogHandle logHandle) {}

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        envp = inheritEnvp(envp);

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
                public int waitFor() throws InterruptedException {
                    try {
                        return exec.waitFor();
                    } catch (InterruptedException e) {
                        exec.destroyForcibly().waitFor();
                        throw e;
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

    private String[] inheritEnvp(@Nullable String[] envp) {
        List<String> systemEnvs = System.getenv().entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());

        systemEnvs.addAll(env);

        if (envp != null) {
            systemEnvs.addAll(List.of(envp));
        }
        return systemEnvs.toArray(String[]::new);

    }
}
