package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgent;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgentConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.transmitter.LzyTransmitterConfig;
import ru.yandex.qe.s3.util.Environment;

import java.net.URI;
import java.nio.file.Path;

public class Start implements LzyCommand {
    private static final Logger LOG = LogManager.getLogger(Start.class);

    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new RuntimeException("Provide lzy server address with -z option to start a task.");
        }
        String serverAddress = parse.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final Path path = Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        final String host = parse.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final String internalHost = parse.getOptionValue('i', host);
        final LzyAgent agent = new LzyServant(
            LzyAgentConfig.builder()
                .serverAddress(URI.create(serverAddress))
                .whiteboardAddress(URI.create(Environment.getLzyWhiteboard()))
                .task(System.getenv("LZYTASK"))
                .token(System.getenv("LZYTOKEN"))
                .bucket(Environment.getBucketName())
                .agentName(host)
                .agentInternalName(internalHost)
                .agentPort(port)
                .root(path)
                .build(),
            LzyTransmitterConfig.builder()
                .access(Environment.getAccessKey())
                .secret(Environment.getSecretKey())
                .endpoint(Environment.getServiceEndpoint())
                .region(Environment.getRegion())
                .pathStyleAccessEnabled(Environment.getPathStyleAccessEnabled())
                .build()
        );

        LOG.info("Starting servant at " + host + ":" + port + path);

        agent.start();
        agent.awaitTermination();
        return 0;
    }
}
