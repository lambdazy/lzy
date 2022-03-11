package ru.yandex.cloud.ml.platform.lzy.servant.env;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public class CondaEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private final PythonEnv pythonEnv;
    private final BaseEnvironment baseEnv;
    private final Lzy.GetS3CredentialsResponse credentials;
    private final String resourcesPath;

    public CondaEnvironment(
        PythonEnv pythonEnv,
        BaseEnvironment baseEnv,
        Lzy.GetS3CredentialsResponse credentials,
        String resourcesPath
    ) throws EnvironmentInstallationException {
        this.pythonEnv = pythonEnv;
        this.baseEnv = baseEnv;
        this.credentials = credentials;
        this.resourcesPath = resourcesPath;

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

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    private void installPyenv() throws EnvironmentInstallationException {
        try {
            final String yamlPath = resourcesPath + "conda.yaml";
            final String yamlBindPath = resourcesPath + "conda.yaml";

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
                String errorMessage = "Failed to update conda env\n"
                    + "  ReturnCode: " + Integer.toString(rc) + "\n"
                    + "  Stdout: " + stdout + "\n\n"
                    + "  Stderr: " + stderr + "\n";
                LOG.error(errorMessage);
                throw new EnvironmentInstallationException(errorMessage);
            }
        } catch (IOException | LzyExecutionException e) {
            throw new EnvironmentInstallationException(e.getMessage());
        }
    }

    private LzyProcess execInEnv(String command, String[] envp) throws LzyExecutionException {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[]{"bash", "-c", "source /root/miniconda3/etc/profile.d/conda.sh && "
                + "conda activate " + pythonEnv.name() + " && " + command};
        return baseEnv.runProcess(bashCmd, envp);
    }

    private LzyProcess execInEnv(String command) throws LzyExecutionException {
        return execInEnv(command, null);
    }

    private List<String> getEnvironmentVariables() {
        Map<String, String> envMap = System.getenv();
        return envMap.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());
    }

    private List<String> getLocalModules() throws LzyExecutionException {
        List<String> envList = new ArrayList<>();
        try {
            LinkedHashMap<String, String> localModules = new LinkedHashMap<>();
            pythonEnv.localModules().forEach(localModule -> localModules.put(localModule.name(), localModule.uri()));
            envList.add("LOCAL_MODULES=" + new ObjectMapper().writeValueAsString(localModules));
            if (credentials.hasAmazon()) {
                envList.add("AMAZON=" + JsonFormat.printer().print(credentials.getAmazon()));
            } else if (credentials.hasAzure()) {
                envList.add("AZURE=" + JsonFormat.printer().print(credentials.getAzure()));
            } else {
                envList.add("AZURE_SAS=" + JsonFormat.printer().print(credentials.getAzureSas()));
            }
        } catch (JsonProcessingException | InvalidProtocolBufferException e) {
            throw new LzyExecutionException(e);
        }
        return envList;
    }

    @Override
    public LzyProcess runProcess(String... command) throws LzyExecutionException {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException {
        try {
            List<String> envList = getEnvironmentVariables();
            envList.addAll(getLocalModules());
            if (envp != null) {
                envList.addAll(Arrays.asList(envp));
            }
            return execInEnv(String.join(" ", command), envList.toArray(String[]::new));
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }

}
