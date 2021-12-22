package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.commons.cli.CommandLine;

public class Cat implements LzyCommand {
    private static final int BUFFER_SIZE = 4096;

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify the name of file to create");
        }

        final Path catPath = Paths.get(command.getArgs()[1]).toAbsolutePath();
        byte[] buffer = new byte[BUFFER_SIZE];
        try (InputStream is = Files.newInputStream(catPath, StandardOpenOption.READ)) {
            int read;
            while ((read = is.read(buffer)) >= 0) {
                System.out.write(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }
}
