package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class CondaEnvConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(CondaEnvConnector.class);

    public CondaEnvConnector(PythonEnv env) throws IOException, InterruptedException {
        installPyenv(env);
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

    public int execAndLog(String command) throws IOException, InterruptedException {
        Process run = exec(command);
        int res = run.waitFor();
        logStream(run.getInputStream(), false);
        logStream(run.getErrorStream(), true);
        return res;
    }

    private void installPyenv(PythonEnv env) throws IOException, InterruptedException {
        Path yaml = Paths.get("/req.yaml");
        Files.createFile(yaml);
        try (FileWriter file = new FileWriter(yaml.toString())){
            file.write(env.yaml());
        }
        execAndLog("conda env update --file " + yaml + " --prune");
        execAndLog("pip install --default-timeout=100 /lzy-python setuptools");
    }


    @Override
    public Process exec(String command) throws IOException {
        LOG.info("Executing command " + command);
        return Runtime.getRuntime().exec(new String[]{
                "bash", "-c",
                "eval \"$(conda shell.bash hook)\" && " +
                        "conda activate default && " +
                        command
        });
    }
}
