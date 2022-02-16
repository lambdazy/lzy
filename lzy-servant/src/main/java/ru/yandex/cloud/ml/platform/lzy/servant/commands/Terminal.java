package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import java.io.FileReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgent;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgentConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyTerminal;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;

public class Terminal implements LzyCommand {

    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new IllegalArgumentException(
                "Provide lzy server address with -z option to start a task.");
        }

        String serverAddress = parse.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final Path privateKeyPath = Paths.get(parse.getOptionValue(
            'k',
            System.getenv("HOME") + "/.ssh/id_rsa"
        ));

        final Path lzyRoot = Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        Runtime.getRuntime().exec("umount " + lzyRoot);
        final String host = parse.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final LzyAgentConfig.LzyAgentConfigBuilder builder = LzyAgentConfig.builder()
            .serverAddress(URI.create(serverAddress))
            .user(System.getenv("USER"))
            .agentName(host)
            .agentInternalName(parse.getOptionValue('i', host))
            .agentPort(port)
            .root(lzyRoot);

        if (Files.exists(privateKeyPath)) {
            try (FileReader keyReader = new FileReader(String.valueOf(privateKeyPath))) {
                String token = JwtCredentials.buildJWT(System.getenv("USER"), keyReader);
                builder.token(token);
            }
        } else {
            builder.token("");
        }
        final LzyAgent terminal = new LzyTerminal(builder.build());

        terminal.start();
        terminal.awaitTermination();
        return 0;
    }
}
