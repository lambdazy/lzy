package ai.lzy.servant.commands;

import ai.lzy.servant.agents.LzyAgent;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import java.net.URI;
import java.nio.file.Path;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.UriScheme;
import ai.lzy.fs.fs.LzyFS;

public class Start implements LzyCommand {
    private static final Logger LOG = LogManager.getLogger(Start.class);
    private static final Options options = new Options();

    static {
        options.addOption(new Option("s", "sid", true, "Servant id"));
        options.addOption(new Option("o", "token", true, "Servant auth token"));
        options.addOption(new Option("b", "bucket", true, "Servant bucket"));
    }

    @Override
    public int execute(CommandLine commandLine) throws Exception {
        if (!commandLine.hasOption('z')) {
            throw new RuntimeException("Provide lzy server address with -z option to start a task.");
        }
        String serverAddress = commandLine.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }

        final CommandLine localCmd = parse(commandLine, options);
        if (localCmd == null) {
            return -1;
        }

        final int agentPort = Integer.parseInt(commandLine.getOptionValue('p', "9999"));
        final int fsPort = commandLine.hasOption('q')
            ? Integer.parseInt(commandLine.getOptionValue('q'))
            : agentPort + 1;

        final Path path = Path.of(commandLine.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        final String host = commandLine.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final LzyServant servant = new LzyServant(
            LzyAgentConfig.builder()
                .serverAddress(URI.create(serverAddress))
                .whiteboardAddress(URI.create(commandLine.getOptionValue('w')))
                .channelManagerAddress(URI.create(commandLine.getOptionValue("channel-manager")))
                .servantId(localCmd.getOptionValue('s'))
                .token(localCmd.getOptionValue('o'))
                .bucket(localCmd.getOptionValue('b'))
                .agentHost(host)
                .agentPort(agentPort)
                .fsPort(fsPort)
                .root(path)
                .scheme(UriScheme.LzyServant.scheme())
                .build()
        );
        servant.awaitTermination();
        servant.close();
        return 0;
    }
}
