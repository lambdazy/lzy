package ai.lzy.servant.env;

import ai.lzy.fs.storage.StorageClient;
import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.model.EnvironmentInstallationException;
import ai.lzy.model.graph.PythonEnv;
import com.google.common.util.concurrent.ListenableFuture;
import net.lingala.zip4j.ZipFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.download.DownloadResult;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CondaEnvironment implements AuxEnvironment {

    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private static final Lock lockForMultithreadingTests = new ReentrantLock();

    private final PythonEnv pythonEnv;
    private final BaseEnvironment baseEnv;
    private final StorageClient storage;
    private final String resourcesPath;
    private final String localModulesDir;

    public CondaEnvironment(
        PythonEnv pythonEnv,
        BaseEnvironment baseEnv,
        StorageClient storage,
        String resourcesPath
    ) throws EnvironmentInstallationException {
        this.pythonEnv = pythonEnv;
        this.baseEnv = baseEnv;
        this.storage = storage;
        this.resourcesPath = resourcesPath;
        this.localModulesDir = Path.of("/", "tmp", "local_modules" + UUID.randomUUID()).toString();

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

            File directory = new File(localModulesDirectoryAbsolutePath());
            boolean created = directory.mkdirs();
            if (!created) {
                String errorMessage = "Failed to create directory to download local modules into;\n"
                    + "  Directory name: " + localModulesDirectoryAbsolutePath() + "\n";
                LOG.error(errorMessage);
                throw new EnvironmentInstallationException(errorMessage);
            }
            LOG.info("CondaEnvironment::installPyenv created directory to download local modules into");
            Transmitter transmitter = storage.transmitter();
            for (var entry : pythonEnv.localModules()) {
                String name = entry.name();
                String url = entry.uri();
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
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lockForMultithreadingTests.unlock();
        }
    }

    private LzyProcess execInEnv(String command, String[] envp) {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[] {"bash", "-c", "eval \"$(conda shell.bash hook)\" && conda activate "
                + pythonEnv.name() + " && " + command};
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
