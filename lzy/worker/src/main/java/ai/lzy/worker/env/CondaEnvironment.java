package ai.lzy.worker.env;

import ai.lzy.fs.storage.StorageClient;
import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.model.EnvironmentInstallationException;
import ai.lzy.model.graph.PythonEnv;
import com.google.common.util.concurrent.ListenableFuture;
import net.lingala.zip4j.ZipFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.download.DownloadResult;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

public class CondaEnvironment implements AuxEnvironment {
    public static boolean RECONFIGURE_CONDA = true;  // Only for tests

    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private static final Lock lockForMultithreadingTests = new ReentrantLock();

    private final PythonEnv pythonEnv;
    private final BaseEnvironment baseEnv;
    private final StorageClient storage;
    private final String resourcesPath;
    private final String localModulesDir;
    private final String envName;

    @Deprecated
    public CondaEnvironment(
        PythonEnv pythonEnv,
        BaseEnvironment baseEnv,
        @Nullable
        StorageClient storage,
        String resourcesPath
    ) throws EnvironmentInstallationException
    {
        this.pythonEnv = pythonEnv;
        this.baseEnv = baseEnv;
        this.storage = storage;
        this.resourcesPath = resourcesPath;
        this.localModulesDir = Path.of("/", "tmp", "local_modules" + UUID.randomUUID()).toString();

        var yaml = new Yaml();
        Map<String, Object> data = yaml.load(pythonEnv.yaml());

        envName = (String) data.getOrDefault("name", "default");

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

    public CondaEnvironment(
        PythonEnv pythonEnv,
        BaseEnvironment baseEnv,
        String resourcesPath
    ) throws EnvironmentInstallationException
    {
        this(pythonEnv, baseEnv, null, resourcesPath);
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

    private void extractFiles(File file, String destinationDirectory) throws IOException {
        LOG.info("CondaEnvironment::extractFiles trying to unzip module archive "
            + file.getAbsolutePath());
        try (ZipFile zipFile = new ZipFile(file.getAbsolutePath())) {
            zipFile.extractAll(destinationDirectory);
        }
    }

    private String localModulesDirectoryAbsolutePath() {
        return localModulesDir;
    }

    private void installPyenv() throws EnvironmentInstallationException {
        lockForMultithreadingTests.lock();
        try {
            if (RECONFIGURE_CONDA) {
                LOG.info("CondaEnvironment::installPyenv trying to install pyenv");
                File condaFile = File.createTempFile("conda", ".yaml");

                try (FileWriter file = new FileWriter(condaFile.getAbsolutePath())) {
                    file.write(pythonEnv.yaml());
                }

                final LzyProcess lzyProcess = execInEnv("conda env update --file " + condaFile.getAbsolutePath());
                final StringBuilder stdout = new StringBuilder();
                final StringBuilder stderr = new StringBuilder();
                try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(lzyProcess.out()))) {
                    reader.lines().forEach(s -> {
                        LOG.info(s);
                        stdout.append(s);
                        stdout.append("\n");
                    });
                }
                try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(lzyProcess.err()))) {
                    reader.lines().forEach(s -> {
                        LOG.error(s);
                        stderr.append(s);
                        stderr.append("\n");
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

                condaFile.delete();
            }

            File directory = new File(localModulesDirectoryAbsolutePath());
            boolean created = directory.mkdirs();
            if (!created) {
                String errorMessage = "Failed to create directory to download local modules into;\n"
                    + "  Directory name: " + localModulesDirectoryAbsolutePath() + "\n";
                LOG.error(errorMessage);
                throw new EnvironmentInstallationException(errorMessage);
            }
            LOG.info("CondaEnvironment::installPyenv created directory to download local modules into");
            for (var entry : pythonEnv.localModules()) {
                String name = entry.name();
                String url = entry.uri();
                LOG.info(
                    "CondaEnvironment::installPyenv installing local module with name " + name + " and url " + url);

                if (storage == null) {

                    File tempFile = File.createTempFile("tmp-file", ".zip");

                    try (InputStream in = new URL(url).openStream()) {
                        Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    }

                    extractFiles(tempFile, localModulesDirectoryAbsolutePath());
                    tempFile.deleteOnExit();

                } else {  // TODO(artolord) Remove this in v2
                    Transmitter transmitter = storage.transmitter();
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
                                extractFiles(tempFile, localModulesDirectoryAbsolutePath());
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
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lockForMultithreadingTests.unlock();
        }
    }

    private LzyProcess execInEnv(String command, String[] envp) {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[] {"bash", "-c", "eval \"$(conda shell.bash hook)\" && conda activate "
                + envName + " && " + command};
        return baseEnv.runProcess(bashCmd, envp);
    }

    private LzyProcess execInEnv(String command) {
        return execInEnv(command, null);
    }

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + localModulesDirectoryAbsolutePath());
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }
        return execInEnv(String.join(" ", command), envList.toArray(String[]::new));
    }

}