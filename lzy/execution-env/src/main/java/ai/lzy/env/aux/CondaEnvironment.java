package ai.lzy.env.aux;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogStream;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
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

    private final Path hostWorkingDir;
    private final Path baseEnvWorkingDir;

    @Nullable
    private String pythonPath;

    @VisibleForTesting
    public static void reconfigureConda(boolean reconfigure) {
        RECONFIGURE_CONDA = reconfigure;
    }

    public CondaEnvironment(BaseEnvironment baseEnv, String condaYaml, Map<String, String> localModules,
                            Path hostWorkingDir, Path baseEnvWorkingDir)
    {
        this.condaYaml = condaYaml;
        this.baseEnv = baseEnv;
        this.localModules = localModules;
        this.hostWorkingDir = hostWorkingDir;
        this.baseEnvWorkingDir = baseEnvWorkingDir;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    public void install(LogStream outStream, LogStream errStream) throws EnvironmentInstallationException {
        lockForMultithreadingTests.lock();
        try {
            final var condaPackageRegistry = baseEnv.getPackageRegistry();

            try {
                installLocalModules(localModules, hostWorkingDir, LOG, outStream, errStream);
            } catch (IOException e) {
                String errorMessage = "Failed to install local modules";
                LOG.error(errorMessage, e);
                errStream.log(errorMessage);
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
                    outStream.log("Trying to install pyenv...");

                    final File condaFileOnHost = Files.createFile(hostWorkingDir.resolve("conda.yaml")).toFile();
                    try (FileWriter file = new FileWriter(condaFileOnHost.getAbsolutePath())) {
                        file.write(envYaml);
                    }

                    var condaFile = baseEnvWorkingDir.resolve("conda.yaml");
                    // Conda env create or update: https://github.com/conda/conda/issues/7819
                    final LzyProcess lzyProcess = execInEnv(
                        "conda env create --file %s || conda env update --file %s".formatted(condaFile, condaFile),
                        outStream);

                    var rc = waitFor(lzyProcess, outStream, errStream);
                    if (rc != 0) {
                        String errorMessage = "Failed to create/update conda env\n"
                            + "  ReturnCode: " + rc + "\n"
                            + "See your stdout/stderr to see more info";
                        LOG.error(errorMessage);
                        errStream.log(errorMessage);
                        throw new EnvironmentInstallationException(errorMessage);
                    }
                    LOG.info("CondaEnvironment::installPyenv successfully updated conda env");
                    outStream.log("Pyenv successfully installed");

                    condaPackageRegistry.notifyInstalled(envYaml);
                    //noinspection ResultOfMethodCallIgnored
                    condaFileOnHost.delete();
                }
            }

            try {
                var proc = execInEnv("echo $PYTHONPATH", null);
                var rc = proc.waitFor();

                if (rc != 0) {
                    LOG.error("Cannot get PYTHONPATH from env");
                    return;
                }

                var path = IOUtils.toString(proc.out(), Charset.defaultCharset()).strip();
                if (!path.isEmpty()) {
                    path += ":";
                }
                path += baseEnvWorkingDir.toAbsolutePath();
                this.pythonPath = path;
            } catch (Exception e) {
                LOG.error("Cannot get PYTHONPATH from env: ", e);
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("CondaEnvironment setup failed", e);
            throw new RuntimeException(e);
        } finally {
            lockForMultithreadingTests.unlock();
        }
    }

    private int waitFor(LzyProcess lzyProcess, LogStream outStream, LogStream errStream)
        throws EnvironmentInstallationException, ExecutionException, InterruptedException
    {
        var futOut = outStream.log(lzyProcess.out());
        var futErr = errStream.log(lzyProcess.err());

        final int rc;
        try {
            rc = lzyProcess.waitFor();
        } catch (InterruptedException e) {
            throw new EnvironmentInstallationException("Environment installation cancelled");
        }

        futOut.get();
        futErr.get();

        return rc;
    }

    private LzyProcess execInEnv(String command, @Nullable String[] envp, @Nullable String workingDir) {
        LOG.info("Executing command " + command);
        assert baseEnvWorkingDir != null;

        var bashCmd = new String[] {
            "bash",
            "-c",
            "eval \"$(conda shell.bash hook)\" && conda activate %s && %s"
                .formatted(envName, command)
        };

        return baseEnv.runProcess(bashCmd, envp, workingDir == null ? baseEnvWorkingDir.toString() : workingDir);
    }

    private LzyProcess execInEnv(String command, @Nullable LogStream out) {
        if (out != null) {
            out.log("RunCmd: %s".formatted(command));
        }
        return execInEnv(command, null, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp, @Nullable String workingDir) {
        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + baseEnvWorkingDir);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }

        var cmd = String.join(" ", command);

        if (pythonPath != null) {
            // Adding export here to prevent conda from updating PYTHONPATH
            cmd = "export PYTHONPATH=" + pythonPath + " && "  + cmd;
        }

        return execInEnv(cmd, envList.toArray(String[]::new), workingDir);
    }

    @Override
    public Path workingDirectory() {
        return hostWorkingDir;
    }
}
