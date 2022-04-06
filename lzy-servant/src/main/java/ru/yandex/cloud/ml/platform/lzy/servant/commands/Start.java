package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import java.net.URI;
import java.nio.file.Path;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgent;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgentConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;

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

        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final Path path = Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        final String host = parse.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final String internalHost = parse.getOptionValue('i', host);
        final LzyAgent agent = new LzyServant(
            LzyAgentConfig.builder()
                .serverAddress(URI.create(serverAddress))
                .whiteboardAddress(URI.create(parse.getOptionValue('w')))
                .task(localCmd.getOptionValue('s', System.getenv("LZYTASK")))
                .token(localCmd.getOptionValue('o', System.getenv("LZYTOKEN")))
                .bucket(localCmd.getOptionValue('b', System.getenv("BUCKET_NAME")))
                .agentName(host)
                .agentInternalName(internalHost)
                .agentPort(port)
                .root(path)
                .build()
        );

        LOG.info("Starting servant at " + host + ":" + port + path);

        agent.start();
        agent.awaitTermination();
        return 0;
    }
}
