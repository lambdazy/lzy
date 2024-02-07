package ai.lzy.env.aux;

import ai.lzy.env.Environment;
import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogStream;
import net.lingala.zip4j.ZipFile;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

public interface AuxEnvironment extends Environment {
    BaseEnvironment base();

    /**
     * Returns path to working directory of environment
     */
    Path workingDirectory();

    @Override
    default void close() throws Exception {
        base().close();
    }

    static void installLocalModules(Map<String, String> localModules, Path localModulesPath, Logger log,
                                    LogStream userOut, LogStream userErr)
        throws EnvironmentInstallationException, IOException
    {
        var msg = "Install python local modules to %s".formatted(localModulesPath);
        log.info(msg);
        userOut.log(msg);
        try {
            Files.createDirectories(localModulesPath);
        } catch (IOException e) {
            String errorMessage = "Failed to create directory to download local modules into;\n"
                + "  Directory name: " + localModulesPath + "\n";
            log.error(errorMessage);
            userErr.log(errorMessage);
            throw new EnvironmentInstallationException(errorMessage);
        }

        log.info("Created directory {} to download local modules into", localModulesPath);
        for (var entry : localModules.entrySet()) {
            String name = entry.getKey();
            String url = entry.getValue();
            log.info("Installing local module with name " + name + " and url " + url);
            userOut.log("Installing local module '%s'".formatted(name));

            File tempFile = File.createTempFile("tmp-file", ".zip");

            try (InputStream in = new URL(url).openStream()) {
                Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

            extractFiles(tempFile, localModulesPath.toString(), log);
            tempFile.delete();
        }
    }

    private static void extractFiles(File file, String destinationDirectory, Logger log) throws IOException {
        log.debug("Trying to unzip module archive {}", file.getAbsolutePath());
        try (ZipFile zipFile = new ZipFile(file.getAbsolutePath())) {
            zipFile.extractAll(destinationDirectory);
        }
    }
}
