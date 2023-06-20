package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class Cat implements LzyCommand {
    private static final int BUFFER_SIZE = 4096;

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Missing filename");
        }

        final Path file = Paths.get(command.getArgs()[1]).toAbsolutePath();
        byte[] buffer = new byte[BUFFER_SIZE];
        try (InputStream is = Files.newInputStream(file, StandardOpenOption.READ)) {
            int read;
            while ((read = is.read(buffer)) >= 0) {
                System.out.write(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        var cmd = new CommandLine.Builder().addArg("fictive");
        for (String arg : args) {
            cmd.addArg(arg);
        }
        System.exit(new Cat().execute(cmd.build()));
    }
}
