package ai.lzy.env.aux;

import ai.lzy.env.logs.LogStream;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public interface PythonBaseEnvironment extends AuxEnvironment {

    static void installLocalModules(Map<String, String> localModules, Path localModulesPath, Logger log,
                                    LogStream userOut, LogStream userErr) throws InstallationException
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
            throw new InstallationException(errorMessage);
        }

        log.info("Created directory {} to download local modules into", localModulesPath);
        for (var entry : localModules.entrySet()) {
            installLocalModule(entry.getKey(), entry.getValue(), localModulesPath, log, userOut, userErr);
        }
    }

    static void installLocalModule(String name, String url, Path path, Logger log, LogStream userOut, LogStream userErr)
        throws InstallationException
    {
        log.info("Installing local module '{}' from {}", name, url);
        userOut.log("Installing local module '%s'".formatted(name));

        File tempFile = null;
        try {
            tempFile = File.createTempFile("tmp-file", ".zip");

            try (InputStream in = new URL(url).openStream()) {
                Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

            log.info("Trying to unzip module archive {}", tempFile.getAbsolutePath());
            extractFiles(tempFile, path);
        } catch (Exception e) {
            log.error("Failed to install local module '{}'", name, e);
            var errorMessage = "Failed to install local module '%s': %s".formatted(name, e.getMessage());
            userErr.log(errorMessage);

            if (tempFile != null) {
                try {
                    //noinspection ResultOfMethodCallIgnored
                    tempFile.delete();
                } catch (Exception ignored) {
                    // ignore
                }
            }

            throw new InstallationException(errorMessage);
        }
    }

    private static void extractFiles(File zip, Path targetDir) throws IOException {
        try (var zipStream = new ZipInputStream(new FileInputStream(zip))) {
            ZipEntry zipEntry = zipStream.getNextEntry();
            while (zipEntry != null) {
                final Path entryTargetPath = targetDir.resolve(zipEntry.getName());
                if (!entryTargetPath.startsWith(targetDir)) {
                    throw new IOException(
                        "Zip entry '%s' is trying to escape target path '%s'".formatted(entryTargetPath, targetDir));
                }
                if (zipEntry.isDirectory()) {
                    Files.createDirectories(entryTargetPath);
                } else {
                    Files.createDirectories(entryTargetPath.getParent());
                    Files.copy(zipStream, entryTargetPath);
                }
                zipEntry = zipStream.getNextEntry();
            }
        }
    }

}
