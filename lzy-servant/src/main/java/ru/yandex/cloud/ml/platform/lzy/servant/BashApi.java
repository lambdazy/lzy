package ru.yandex.cloud.ml.platform.lzy.servant;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Start;

public class BashApi {
    private static final Options options = new Options();
    private static final Logger LOG = LogManager.getLogger(BashApi.class);

    static {
        options.addOption(new Option("p", "port", true,
            "gRPC port setting"));
        options.addOption(new Option("a", "auth", true,
            "Enforce auth"));
        options.addOption(new Option("z", "lzy-address", true,
            "Lzy server address [host:port]"));
        options.addOption(new Option("w", "lzy-whiteboard", true, "Lzy whiteboard address [host:port]"));
        options.addOption(new Option("m", "lzy-mount", true,
            "Lzy FS mount point"));
        options.addOption(new Option("h", "host", true,
            "Servant host name"));
        options.addOption(new Option("i", "internal-host", true,
            "Servant host name for connection from another servants"));
        options.addOption(new Option("k", "private-key", true,
            "Path to private key for user auth"));
    }

    public static void main(String[] args) {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        String commandStr = "lzy";
        try {
            final CommandLine parse = cliParser.parse(options, args, true);
            if (parse.getArgs().length > 0) {
                commandStr = parse.getArgs()[0];
                final LzyCommand.Commands command = LzyCommand.Commands.valueOf(commandStr);
                System.exit(command.execute(parse));
            } else {
                new Start().execute(parse);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp(commandStr, options);
            System.exit(-1);
        } catch (Exception e) {
            LOG.error("Error while executing: " + String.join(" ", args), e);
        }
    }
}
