package ai.lzy.env.aux;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogHandle;
import jakarta.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PlainPythonEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(PlainPythonEnvironment.class);

    private final BaseEnvironment baseEnv;
    private final String localModulesPathPrefix;
    private final Map<String, String> localModules;
    private Path localModulesPath;

    public PlainPythonEnvironment(BaseEnvironment baseEnv, Map<String, String> localModules,
                                  String localModulesPathPrefix) {
        this.localModules = localModules;
        this.baseEnv = baseEnv;
        this.localModulesPathPrefix = localModulesPathPrefix;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    @Override
    public void install(LogHandle logHandle) throws EnvironmentInstallationException {
        var proc = baseEnv.runProcess("python", "--version");
        final int res;

        try {
            res = proc.waitFor();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while installing env", e);
            throw new EnvironmentInstallationException("Interrupted while installing env");
        }

        if (res != 0) {
            String err = "";
            try {
                err = IOUtils.toString(proc.err(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.error("Error while getting stderr of process: ", e);
            }

            logHandle.logErr("Cannot get python version. It can be not provided. STDERR: {}", err);
            throw new EnvironmentInstallationException("Python not found. Maybe your docker not contains it");
        }

        final String out;
        try {
            out = IOUtils.toString(proc.out(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Error while getting stdout of process: ", e);
            throw new EnvironmentInstallationException("Cannot get version of provided python");
        }

        logHandle.logErr("Using provided python with version \"{}\"", out);

        try {
            this.localModulesPath = AuxEnvironment.installLocalModules(localModules, localModulesPathPrefix, LOG);
        } catch (IOException e) {
            LOG.error("Cannot install local modules", e);
            throw new EnvironmentInstallationException("Cannot install local modules: " + e.getMessage());
        }
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp, @Nullable Path workingDirectory) {
        if (workingDirectory != null) {
            throw new NotImplementedException("Cannot change working directory in python env");
        }

        var list = new ArrayList<>(List.of("cd", localModulesPath.toString(), "&&"));
        list.addAll(List.of(command));

        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + localModulesPath);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }

        return baseEnv.runProcess(new String[]{"/bin/bash", "-c", String.join(" ", list)},
            envList.toArray(String[]::new),
            localModulesPath);
    }

    @Override
    public Path workingDirectory() {
        return localModulesPath;
    }
}
