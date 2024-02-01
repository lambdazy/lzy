package ai.lzy.env.base;

import ai.lzy.env.Environment;
import ai.lzy.env.logs.LogStream;
import jakarta.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessEnvironment extends BaseEnvironment {
    private final List<String> env = new ArrayList<>();

    public ProcessEnvironment() {
        super();
    }

    public ProcessEnvironment withEnv(Map<String, String> env) {
        for (var e: env.entrySet()) {
            this.env.add(e.getKey() + "=" + e.getValue());
        }
        return this;
    }

    @Override
    public void install(LogStream outStream, LogStream errStream) {}

    @Override
    public Environment.LzyProcess runProcess(String[] command, String[] envp, @Nullable String workingDir) {
        envp = inheritEnvp(envp);

        try {
            var builder = new ProcessBuilder()
                .command(command);

            if (workingDir != null) {
                builder.directory(new File(workingDir));
            }

            for (var env: envp) {
                var res = env.split("=");
                builder.environment().put(res[0], res[1]);
            }

            var exec = builder.start();

            return new Environment.LzyProcess() {
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
