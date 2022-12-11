package ai.lzy.fs;

import ai.lzy.fs.commands.BuiltinCommandHolder;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BashApi {
    private static final Options options = new Options();
    private static final Logger LOG = LogManager.getLogger(BashApi.class);

    static {
        options.addRequiredOption("z", "lzy-address", true, "Lzy server address [host:port]");
        options.addRequiredOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addRequiredOption("m", "lzy-mount", true, "Lzy FS mount point");
        options.addRequiredOption("a", "auth", true, "Enforce auth");
        options.addRequiredOption("p", "lzy-fs-port", true, "LzyFs port");
        options.addRequiredOption("i", "agent-id", true, "Agent id (system option)");
        options.addOption(new Option("k", "private-key", true, "Path to private key for user auth"));
        options.addOption(new Option("w", "lzy-whiteboard", true, "Lzy whiteboard address [host:port]"));
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    public static int execute(String[] args) {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        String commandStr = "lzy";
        try {
            LOG.info("Parsed bash command {}", String.join(" ", args));
            final CommandLine parse = cliParser.parse(options, args, true);
            if (parse.getArgs().length > 0) {
                commandStr = parse.getArgs()[0];
                final BuiltinCommandHolder command = BuiltinCommandHolder.valueOf(commandStr);
                return command.execute(parse);
            } else {
                throw new RuntimeException("Missed command");
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            cliHelp.printHelp(commandStr, options);
            return -1;
        } catch (Exception e) {
            LOG.error("Error while executing: " + String.join(" ", args), e);
            return -1;
        }
    }
}
