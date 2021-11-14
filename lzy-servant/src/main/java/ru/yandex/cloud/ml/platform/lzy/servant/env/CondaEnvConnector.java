package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class CondaEnvConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(CondaEnvConnector.class);
    private final PythonEnv env;
    private final AtomicBoolean envInstalled = new AtomicBoolean(false);

    public CondaEnvConnector(PythonEnv env) {
        this.env = env;
    }

    private void logStream(InputStream stream, boolean warn) {
        Scanner scanner = new Scanner(stream);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (warn) {
                LOG.warn(line);
            } else {
                LOG.info(line);
            }
        }
    }

    private int execAndLog(String command) throws IOException, InterruptedException {
        Process run = exec(command);
        int res = run.waitFor();
        logStream(run.getInputStream(), false);
        logStream(run.getErrorStream(), true);
        return res;
    }

    private void installPyenv() throws IOException, InterruptedException {
        final File yaml = File.createTempFile("conda", "req.yaml");
        try (FileWriter file = new FileWriter(yaml.getAbsolutePath())) {
            file.write(env.yaml());
        }
        // --prune removes packages not specified in yaml, so probably it has not to be there
        final int rc = execAndLog("conda env update --file " + yaml.getAbsolutePath()); // + " --prune");
        if (rc > 0) {
            throw new IOException("Failed to update conda env");
        }
    }


    @Override
    public Process exec(String command) throws IOException, InterruptedException {
        if (envInstalled.compareAndSet(false, true)) {
            installPyenv();
        }
        LOG.info("Executing command " + command);
        return Runtime.getRuntime().exec(new String[]{
                "bash", "-c",
                "eval \"$(conda shell.bash hook)\" && " +
                        "conda activate " + env.name() + " && " +
                        command
        });
    }
}
