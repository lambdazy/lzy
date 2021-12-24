package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CondaEnvironment implements Environment {
    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private final PythonEnv env;
    private final AtomicBoolean envInstalled = new AtomicBoolean(false);

    public CondaEnvironment(PythonEnv env) {
        this.env = env;
    }

    private void installPyenv() throws EnvironmentInstallationException {
        try {
            final File yaml = File.createTempFile("conda", "req.yaml");
            try (FileWriter file = new FileWriter(yaml.getAbsolutePath())) {
                file.write(env.yaml());
            }
            // --prune removes packages not specified in yaml, so probably it has not to be there
            final Process run = execInEnv("conda env update --file " + yaml.getAbsolutePath()); // + " --prune");
            final int rc = run.waitFor();
            final String stdout = IOUtils.toString(run.getInputStream());
            final String stderr = IOUtils.toString(run.getErrorStream());
            LOG.info(stdout);
            LOG.error(stderr);
            if (run.exitValue() != 0) {
                throw new EnvironmentInstallationException(
                    String.format(
                        "Failed to update conda env\n\nSTDOUT: %s \n\nSTDERR: %s",
                        stdout, stderr
                    )
                );
            }
        } catch (IOException | InterruptedException e) {
            throw new EnvironmentInstallationException(e.getMessage());
        }
    }

    private Process execInEnv(String command) throws IOException {
        LOG.info("Executing command " + command);
        return Runtime.getRuntime().exec(new String[]{
            "bash", "-c",
            "eval \"$(conda shell.bash hook)\" && " +
                "conda activate " + env.name() + " && " +
                command
        });
    }

    @Override
    public Process exec(String command) throws EnvironmentInstallationException, LzyExecutionException {
        if (envInstalled.compareAndSet(false, true)) {
            final long pyEnvInstallStart = System.currentTimeMillis();
            installPyenv();
            final long pyEnvInstallFinish = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "time for installing py env millis",
                    Map.of(),
                    pyEnvInstallFinish - pyEnvInstallStart
                )
            );
        }
        try {
           return execInEnv(command);
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }
}
