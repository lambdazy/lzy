package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public class CondaEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private final PythonEnv pythonEnv;
    private final BaseEnvironment baseEnv;
    private final AtomicBoolean envPreparingStarted = new AtomicBoolean(false);
    private final AtomicBoolean envPrepared = new AtomicBoolean(false);

    public CondaEnvironment(PythonEnv pythonEnv, BaseEnvironment baseEnv) {
        this.pythonEnv = pythonEnv;
        this.baseEnv = baseEnv;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    private void installPyenv() throws EnvironmentInstallationException {
        try {
            final String yamlPath = "/tmp/resources/conda.yaml";
            final String yamlBindPath = "/tmp/resources/conda.yaml";

            try (FileWriter file = new FileWriter(yamlPath)) {
                file.write(pythonEnv.yaml());
            }
            // --prune removes packages not specified in yaml, so probably it has not to be there
            final LzyProcess lzyProcess = execInEnv("conda env update --file " + yamlBindPath); // + " --prune");
            final StringBuilder stdout = new StringBuilder();
            final StringBuilder stderr = new StringBuilder();
            try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(lzyProcess.out()))) {
                reader.lines().forEach(s -> {
                    LOG.info(s);
                    stdout.append(s);
                });
            }
            try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(lzyProcess.err()))) {
                reader.lines().forEach(s -> {
                    LOG.error(s);
                    stderr.append(s);
                });
            }
            final int rc = lzyProcess.waitFor();
            if (rc != 0) {
                throw new EnvironmentInstallationException(
                    String.format(
                        "Failed to update conda env\n\nReturnCode: %s \n\nStdout: %s \n\nStderr: %s",
                        rc, stdout, stderr
                    )
                );
            }
        } catch (IOException | LzyExecutionException e) {
            throw new EnvironmentInstallationException(e.getMessage());
        }
    }

    private LzyProcess execInEnv(String command, String[] envp) throws LzyExecutionException {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[]{"bash", "-c",
            "source /root/miniconda3/etc/profile.d/conda.sh && " +
            "conda activate " + pythonEnv.name() + " && " + command};
        return baseEnv.runProcess(bashCmd, envp);
    }

    private LzyProcess execInEnv(String command) throws LzyExecutionException {
        return execInEnv(command, null);
    }

    @Override
    public void prepare() throws EnvironmentInstallationException {
        baseEnv.prepare();
        if (envPreparingStarted.compareAndSet(false, true)) {
            final long pyEnvInstallStart = System.currentTimeMillis();
            installPyenv();
            final long pyEnvInstallFinish = System.currentTimeMillis();
            envPrepared.set(true);
            MetricEventLogger.log(
                new MetricEvent(
                    "time for installing py env millis",
                    Map.of("metric_type", "task_metric"),
                    pyEnvInstallFinish - pyEnvInstallStart
                )
            );
        }
    }

    @Override
    public LzyProcess runProcess(String... command) throws LzyExecutionException {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException {
        assert envPrepared.get() : "Environment not prepared";
        try {
            return execInEnv(String.join(" ", command), envp);
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }


}
