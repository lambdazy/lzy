package ai.lzy.env.aux;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogHandle;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static ai.lzy.env.aux.AuxEnvironment.installLocalModules;

public class CondaEnvironment implements AuxEnvironment {
    private static volatile boolean RECONFIGURE_CONDA = true;  // Only for tests

    private static final Logger LOG = LogManager.getLogger(CondaEnvironment.class);
    private static final Lock lockForMultithreadingTests = new ReentrantLock();

    private final String condaYaml;
    private final BaseEnvironment baseEnv;
    private final String envName;
    private final String resourcesPath;
    private final String localModulesPathPrefix;
    private final Map<String, String> localModules;

    @Nullable
    private Path localModulesAbsolutePath = null;

    @VisibleForTesting
    public static void reconfigureConda(boolean reconfigure) {
        RECONFIGURE_CONDA = reconfigure;
    }

    public CondaEnvironment(BaseEnvironment baseEnv, String condaYaml, Map<String, String> localModules,
                            String resourcesPath, String localModulesPath)
    {
        this.resourcesPath = resourcesPath;
        this.localModulesPathPrefix = localModulesPath;
        this.condaYaml = condaYaml;
        this.baseEnv = baseEnv;
        this.localModules = localModules;

        var yaml = new Yaml();
        Map<String, Object> data = yaml.load(condaYaml);

        envName = (String) data.getOrDefault("name", "default");
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    public void install(LogHandle logHandle) throws EnvironmentInstallationException {
        lockForMultithreadingTests.lock();
        try {
            final var condaPackageRegistry = baseEnv.getPackageRegistry();

            try {
                this.localModulesAbsolutePath = installLocalModules(localModules, localModulesPathPrefix, LOG);
            } catch (IOException e) {
                String errorMessage = "Failed to install local modules";
                LOG.error("Fail to install local modules. \n", e);
                throw new EnvironmentInstallationException(errorMessage);
            }

            if (RECONFIGURE_CONDA) {
                if (condaPackageRegistry.isInstalled(condaYaml)) {

                    LOG.info("Conda env {} already configured, skipping", envName);

                } else {

                    LOG.info("CondaEnvironment::installPyenv trying to install pyenv");

                    Path condaPath = Path.of(resourcesPath, UUID.randomUUID().toString());
                    Files.createDirectories(condaPath);
                    final File condaFile = Files.createFile(Path.of(condaPath.toString(), "conda.yaml")).toFile();

                    try (FileWriter file = new FileWriter(condaFile.getAbsolutePath())) {
                        file.write(condaYaml);
                    }

                    // Conda env create or update: https://github.com/conda/conda/issues/7819
                    final LzyProcess lzyProcess = execInEnv(
                        String.format("conda env create --file %s  || conda env update --file %s",
                            condaFile.getAbsolutePath(),
                            condaFile.getAbsolutePath())
                    );

                    var futOut = logHandle.logOut(lzyProcess.out());
                    var futErr = logHandle.logErr(lzyProcess.err());

                    final int rc;
                    try {
                        rc = lzyProcess.waitFor();
                    } catch (InterruptedException e) {
                        throw new EnvironmentInstallationException("Environment installation cancelled");
                    }

                    futOut.get();
                    futErr.get();
                    if (rc != 0) {
                        String errorMessage = "Failed to create/update conda env\n"
                            + "  ReturnCode: " + rc + "\n"
                            + "See your stdout/stderr to see more info";
                        LOG.error(errorMessage);
                        throw new EnvironmentInstallationException(errorMessage);
                    }
                    LOG.info("CondaEnvironment::installPyenv successfully updated conda env");

                    condaPackageRegistry.notifyInstalled(condaYaml);
                    //noinspection ResultOfMethodCallIgnored
                    condaFile.delete();
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            lockForMultithreadingTests.unlock();
        }
    }

    private LzyProcess execInEnv(String command, @Nullable String[] envp) {
        LOG.info("Executing command " + command);
        assert localModulesAbsolutePath != null;

        var bashCmd = new String[] {
            "/bin/bash",
            "-c",
            "cd %s && eval \"$(conda shell.bash hook)\" && conda activate %s && %s"
                .formatted(localModulesAbsolutePath, envName, command)
        };

        return baseEnv.runProcess(bashCmd, envp);
    }

    private LzyProcess execInEnv(String command) {
        return execInEnv(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp) {
        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + localModulesAbsolutePath);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }
        return execInEnv(String.join(" ", command), envList.toArray(String[]::new));
    }

    @Override
    public Path workingDirectory() {
        return localModulesAbsolutePath;
    }
}
