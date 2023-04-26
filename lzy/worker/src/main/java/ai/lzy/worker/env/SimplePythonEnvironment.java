package ai.lzy.worker.env;

import ai.lzy.v1.common.LME;
import ai.lzy.worker.StreamQueue;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimplePythonEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(SimplePythonEnvironment.class);

    private final BaseEnvironment base;
    private final LME.PythonEnv env;
    private final String localModulesPathPrefix;
    private Path localModulesPath;

    public SimplePythonEnvironment(BaseEnvironment base, LME.PythonEnv env, String localModulesPathPrefix) {
        this.env = env;
        this.base = base;
        this.localModulesPathPrefix = localModulesPathPrefix;
    }

    @Override
    public BaseEnvironment base() {
        return base;
    }

    @Override
    public void install(StreamQueue.LogHandle logHandle) throws EnvironmentInstallationException {
        logHandle.logErr("*WARNING* Using plain python environment instead of conda." +
            " Your packages will not be installed");

        var proc = base.runProcess("python", "--version");
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
            this.localModulesPath = AuxEnvironment.installLocalModules(env, localModulesPathPrefix, LOG);
        } catch (IOException e) {
            LOG.error("Cannot install local modules", e);
            throw new EnvironmentInstallationException("Cannot install local modules: " + e.getMessage());
        }
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        var list = new ArrayList<>(List.of("cd", localModulesPath.toString(), "&&"));
        list.addAll(List.of(command));

        List<String> envList = new ArrayList<>();
        envList.add("LOCAL_MODULES=" + localModulesPath);
        if (envp != null) {
            envList.addAll(Arrays.asList(envp));
        }
        return base.runProcess(String.join(" ", list), envList.toArray(String[]::new));
    }
}
