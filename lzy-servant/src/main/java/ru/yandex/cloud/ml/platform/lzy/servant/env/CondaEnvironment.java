package ru.yandex.cloud.ml.platform.lzy.servant.env;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public class CondaEnvironment implements Environment {
    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private final PythonEnv env;
    private final AtomicBoolean envInstalled = new AtomicBoolean(false);
    private final Lzy.GetS3CredentialsResponse credentials;

    public CondaEnvironment(PythonEnv env, Lzy.GetS3CredentialsResponse credentials) {
        this.env = env;
        this.credentials = credentials;
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

    private Process execInEnv(String command, String[] envp) throws IOException {
        LOG.info("Executing command " + command);
        return Runtime.getRuntime().exec(new String[] {
            "bash", "-c",
            "eval \"$(conda shell.bash hook)\" && "
                + "conda activate " + env.name() + " && "
                + command
        }, envp);
    }

    private Process execInEnv(String command) throws IOException {
        return execInEnv(command, null);
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
                    Map.of("metric_type", "task_metric"),
                    pyEnvInstallFinish - pyEnvInstallStart
                )
            );
        }
        try {
            List<String> envList = getEnvironmentVariables();
            envList.addAll(getLocalModules());
            return execInEnv(command, envList.toArray(String[]::new));
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }
    }

    private List<String> getEnvironmentVariables() {
        Map<String, String> envMap = System.getenv();
        return envMap.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());
    }

    private List<String> getLocalModules() throws EnvironmentInstallationException {
        List<String> envList = new ArrayList<>();
        try {
            LinkedHashMap<String, String> localModules = new LinkedHashMap<>();
            env.localModules().forEach(localModule -> localModules.put(localModule.name(), localModule.uri()));
            envList.add("LOCAL_MODULES=" + new ObjectMapper().writeValueAsString(localModules));
            if (credentials.hasAmazon()) {
                envList.add("AMAZON=" + JsonFormat.printer().print(credentials.getAmazon()));
            } else if (credentials.hasAzure()) {
                envList.add("AZURE=" + JsonFormat.printer().print(credentials.getAzure()));
            } else {
                envList.add("AZURE_SAS=" + JsonFormat.printer().print(credentials.getAzureSas()));
            }
        } catch (JsonProcessingException | InvalidProtocolBufferException e) {
            throw new EnvironmentInstallationException(e.getMessage());
        }
        return envList;
    }
}
