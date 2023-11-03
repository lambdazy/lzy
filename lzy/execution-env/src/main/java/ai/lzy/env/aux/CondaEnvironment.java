package ai.lzy.env.aux;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogStream;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private String envName;
    private final Map<String, String> localModules;

    private final Path workingDir;

    @VisibleForTesting
    public static void reconfigureConda(boolean reconfigure) {
        RECONFIGURE_CONDA = reconfigure;
    }

    public CondaEnvironment(BaseEnvironment baseEnv, String condaYaml, Map<String, String> localModules,
                            Path workingDir)
    {
        this.condaYaml = condaYaml;
        this.baseEnv = baseEnv;
        this.localModules = localModules;
        this.workingDir = workingDir;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    public void install(LogStream systemStream) throws EnvironmentInstallationException {
        lockForMultithreadingTests.lock();
        try {
            final var condaPackageRegistry = baseEnv.getPackageRegistry();

            try {
                installLocalModules(localModules, workingDir, LOG);
            } catch (IOException e) {
                String errorMessage = "Failed to install local modules";
                LOG.error("Fail to install local modules. \n", e);
                throw new EnvironmentInstallationException(errorMessage);
            }

            if (!RECONFIGURE_CONDA) {  // Only for tests
                envName = "py39";
            } else {
                condaPackageRegistry.init();

                envName = condaPackageRegistry.resolveEnvName(condaYaml);
                var envYaml = condaPackageRegistry.buildCondaYaml(condaYaml);

                if (envYaml == null) {
                    LOG.info("Conda env {} already configured, skipping", envName);

                } else {
                    LOG.info("CondaEnvironment::installPyenv trying to install pyenv");

                    final File condaFile = Files.createFile(workingDir.resolve("conda.yaml")).toFile();

                    try (FileWriter file = new FileWriter(condaFile.getAbsolutePath())) {
                        file.write(envYaml);
                    }

                    // Conda env create or update: https://github.com/conda/conda/issues/7819
                    final LzyProcess lzyProcess = execInEnv(
                        String.format("conda env create --file %s || conda env update --file %s",
                            condaFile.getAbsolutePath(),
                            condaFile.getAbsolutePath()));

                    var futOut = systemStream.log(lzyProcess.out());
                    var futErr = systemStream.log(lzyProcess.err());

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
                        systemStream.log(errorMessage);
                        throw new EnvironmentInstallationException(errorMessage);
                    }
                    LOG.info("CondaEnvironment::installPyenv successfully updated conda env");

                    condaPackageRegistry.notifyInstalled(envYaml);
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
        assert workingDir != null;

        var bashCmd = new String[] {
            "bash",
            "-c",
            "cd %s && eval \"$(conda shell.bash hook)\" && conda activate %s && %s"
                .formatted(workingDir, envName, command)
        };

        return baseEnv.runProcess(bashCmd, envp);
    }

    private LzyProcess execInEnv(String command) {
        return execInEnv(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp) {
        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + workingDir);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }
        return execInEnv(String.join(" ", command), envList.toArray(String[]::new));
    }

    @Override
    public Path workingDirectory() {
        return workingDir;
    }
}
