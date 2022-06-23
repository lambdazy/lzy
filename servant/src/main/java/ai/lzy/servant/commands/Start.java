package ai.lzy.servant.commands;

import ai.lzy.servant.agents.LzyAgent;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import java.net.URI;
import java.nio.file.Path;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.UriScheme;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyFS;

public class Start implements LzyCommand {
    private static final Logger LOG = LogManager.getLogger(Start.class);
    private static final Options options = new Options();

    static {
        options.addOption(new Option("s", "sid", true, "Servant id"));
        options.addOption(new Option("o", "token", true, "Servant auth token"));
        options.addOption(new Option("b", "bucket", true, "Servant bucket"));
    }

    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new RuntimeException("Provide lzy server address with -z option to start a task.");
        }
        String serverAddress = parse.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }

        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, parse.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("channel", options);
            return -1;
        }

        final int agentPort = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final int fsPort = parse.hasOption('q') ? Integer.parseInt(parse.getOptionValue('q')) : agentPort + 1;

        final Path path = Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        final String host = parse.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final LzyAgent agent = new LzyServant(
            LzyAgentConfig.builder()
                .serverAddress(URI.create(serverAddress))
                .whiteboardAddress(URI.create(parse.getOptionValue('w')))
                .servantId(localCmd.getOptionValue('s'))
                .token(localCmd.getOptionValue('o'))
                .bucket(localCmd.getOptionValue('b'))
                .agentHost(host)
                .agentPort(agentPort)
                .fsPort(fsPort)
                .root(path)
                .build()
        );

        LOG.info("Starting servant at {}://{}:{}/{} with fs at {}:{}",
            UriScheme.LzyServant.scheme(), host, agent, path, host, fsPort);

        agent.start();
        agent.awaitTermination();
        return 0;
    }
}
