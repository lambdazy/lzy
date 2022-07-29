package ai.lzy.servant;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.servant.commands.ServantCommandHolder;
import ai.lzy.servant.commands.Start;

public class BashApi {
    private static final Options options = new Options();
    private static final Logger LOG = LogManager.getLogger(BashApi.class);

    static {
        options.addOption("p", "port", true, "Agent gRPC port.");
        options.addOption("q", "fs-port", true, "LzyFs gRPC port.");
        options.addOption("a", "auth", true, "Enforce auth");
        options.addOption("z", "lzy-address", true, "Lzy server address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("w", "lzy-whiteboard", true, "Lzy whiteboard address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");
        options.addOption("h", "host", true, "Servant and FS host name");
        options.addOption("k", "private-key", true, "Path to private key for user auth");
        options.addOption("i", "agent-id", true, "Agent id (private option, passed automatically");
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    public static int execute(String[] args) {
        LOG.debug("Going to execute: " + String.join(" ", args));
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        String commandStr = "lzy";
        try {
            final CommandLine parse = cliParser.parse(options, args, true);
            if (parse.getArgs().length > 0) {
                commandStr = parse.getArgs()[0];
                final ServantCommandHolder command = ServantCommandHolder.valueOf(commandStr);
                return command.execute(parse);
            } else {
                return new Start().execute(parse);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp(commandStr, options);
            return -1;
        } catch (Exception e) {
            LOG.error("Error while executing: " + String.join(" ", args), e);
            return -1;
        }
    }
}
