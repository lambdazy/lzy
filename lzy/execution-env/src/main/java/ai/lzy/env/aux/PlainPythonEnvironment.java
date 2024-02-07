package ai.lzy.env.aux;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogStream;
import jakarta.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ai.lzy.env.aux.AuxEnvironment.installLocalModules;

public class PlainPythonEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(PlainPythonEnvironment.class);

    private final BaseEnvironment baseEnv;
    private final Map<String, String> localModules;
    private final Path hostWorkingDir;
    private final Path baseEnvWorkingDir;

    public PlainPythonEnvironment(BaseEnvironment baseEnv, Map<String, String> localModules, Path hostWorkingDir,
                                  Path baseEnvWorkingDir)
    {
        this.localModules = localModules;
        this.baseEnv = baseEnv;
        this.hostWorkingDir = hostWorkingDir;
        this.baseEnvWorkingDir = baseEnvWorkingDir;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    @Override
    public void install(LogStream outStream, LogStream errStream) throws EnvironmentInstallationException {
        var proc = baseEnv.runProcess("python", "--version");
        int res;

        try {
            res = proc.waitFor();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while installing env", e);
            throw new EnvironmentInstallationException("Interrupted while installing env");
        }

        if (res != 0) {
            var error = "";
            try {
                error = IOUtils.toString(proc.err(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.error("Error while getting stderr of process: ", e);
            }

            error = "Cannot execute `python --version`: %s (rc=%d). Try `python3` ...".formatted(error, res);
            LOG.error(error);
            errStream.log(error);

            proc = baseEnv.runProcess("python3", "--version");
            try {
                res = proc.waitFor();
            } catch (InterruptedException e) {
                LOG.error("Interrupted while installing env", e);
                throw new EnvironmentInstallationException("Interrupted while installing env");
            }
        }

        if (res != 0) {
            String error = "";
            try {
                error = IOUtils.toString(proc.err(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.error("Error while getting stderr of process: ", e);
            }

            var msg = "Cannot get python version. It can be not provided. STDERR: " + error;
            LOG.error(msg);
            errStream.log(msg);
            throw new EnvironmentInstallationException(
                "Python (and python3) not found. Maybe your docker doesn't contain it");
        }

        final String output;
        try {
            output = IOUtils.toString(proc.out(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Error while getting stdout of process: ", e);
            throw new EnvironmentInstallationException("Cannot get version of provided python");
        }

        LOG.info("Using provided python with version \"{}\"", output);

        try {
            installLocalModules(localModules, hostWorkingDir, LOG, outStream, errStream);
        } catch (IOException e) {
            LOG.error("Cannot install local modules", e);
            throw new EnvironmentInstallationException("Cannot install local modules: " + e.getMessage());
        }
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp, @Nullable String workingDir) {
        var list = new ArrayList<>(List.of(command));

        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + baseEnvWorkingDir);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }

        return baseEnv.runProcess(new String[]{"bash", "-c", String.join(" ", list)},
            envList.toArray(String[]::new),
            workingDir == null ? baseEnvWorkingDir.toString() : workingDir);
    }

    @Override
    public Path workingDirectory() {
        return baseEnvWorkingDir;
    }
}
