package ru.yandex.cloud.ml.platform.lzy.servant.env;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage.SnapshotStorage;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.download.DownloadResult;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public class CondaEnvironment implements AuxEnvironment {

    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private final PythonEnv pythonEnv;
    private final BaseEnvironment baseEnv;
    private final SnapshotStorage storage;
    private final String resourcesPath;

    public CondaEnvironment(
        PythonEnv pythonEnv,
        BaseEnvironment baseEnv,
        SnapshotStorage storage,
        String resourcesPath
    ) throws EnvironmentInstallationException {
        this.pythonEnv = pythonEnv;
        this.baseEnv = baseEnv;
        this.storage = storage;
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

    private void readToFile(File file, InputStream stream) throws IOException {
        try (FileOutputStream output = new FileOutputStream(file.getAbsolutePath(), true)) {
            byte[] buffer = new byte[4096];
            int len = 0;
            while (len != -1) {
                output.write(buffer, 0, len);
                len = stream.read(buffer);
            }
        }
    }

    private void extractFiles(File file, String destinationDirectory) throws ZipException {
        LOG.info("CondaEnvironment::extractFiles trying to unzip module archive "
            + file.getAbsolutePath());
        ZipFile zipFile = new ZipFile(file.getAbsolutePath());
        zipFile.extractAll(destinationDirectory);
    }

    private void installPyenv() throws EnvironmentInstallationException {
        try {

            LOG.info("CondaEnvironment::installPyenv trying to install pyenv");
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
                    + "  ReturnCode: " + rc + "\n"
                    + "  Stdout: " + stdout + "\n\n"
                    + "  Stderr: " + stderr + "\n";
                LOG.error(errorMessage);
                throw new EnvironmentInstallationException(errorMessage);
            }
            LOG.info("CondaEnvironment::installPyenv successfully updated conda env");

            LinkedHashMap<String, String> localModules = new LinkedHashMap<>();
            pythonEnv.localModules().forEach(localModule -> localModules.put(localModule.name(), localModule.uri()));
            String directoryName = "/local_modules";
            File directory = new File(directoryName);
            boolean created = directory.mkdirs();
            if (!created) {
                String errorMessage = "Failed to create directory to download local modules into;\n"
                    + "  Directory name: " + directoryName + "\n";
                LOG.error(errorMessage);
                throw new EnvironmentInstallationException(errorMessage);
            }
            LOG.info("CondaEnvironment::installPyenv created directory to download local modules into");
            Transmitter transmitter = storage.transmitter();
            for (var entry : localModules.entrySet()) {
                String name = entry.getKey();
                String url = entry.getValue();
                LOG.info(
                    "CondaEnvironment::installPyenv installing local module with name " + name + " and url " + url);

                String bucket = storage.bucket(URI.create(url));
                String key = storage.key(URI.create(url));

                File tempFile = File.createTempFile("tmp-file", ".zip");
                LOG.info("CondaEnvironment::installPyenv trying to download module from storage");
                ListenableFuture<DownloadResult<Void>> resultFuture = transmitter.downloadC(
                    new DownloadRequestBuilder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                    data -> {
                        InputStream stream = data.getInputStream();
                        readToFile(tempFile, stream);
                        stream.close();
                        extractFiles(tempFile, directoryName);
                    }
                );
                DownloadResult<Void> result = resultFuture.get();
                if (result.getDownloadState().getTransferStatus() != TransferStatus.DONE) {
                    String errorMessage = "Failed to unzip local module " + name;
                    LOG.error(errorMessage);
                    throw new EnvironmentInstallationException(errorMessage);
                }
                tempFile.deleteOnExit();
            }
        } catch (IOException | LzyExecutionException | ExecutionException | InterruptedException e) {
            throw new EnvironmentInstallationException(e.getMessage());
        }
    }

    private LzyProcess execInEnv(String command, String[] envp) throws LzyExecutionException {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[] {"bash", "-c", "source /root/miniconda3/etc/profile.d/conda.sh && "
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

    @Override
    public LzyProcess runProcess(String... command) throws LzyExecutionException {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException {
        try {
            List<String> envList = getEnvironmentVariables();
            if (envp != null) {
                envList.addAll(Arrays.asList(envp));
            }
            return execInEnv(String.join(" ", command), envList.toArray(String[]::new));
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }

}
