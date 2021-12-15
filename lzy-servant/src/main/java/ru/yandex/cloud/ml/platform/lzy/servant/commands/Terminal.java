package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Credentials;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgent;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgentConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyTerminal;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;

public class Terminal implements LzyCommand {
    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new IllegalArgumentException("Provide lzy server address with -z option to start a task.");
        }

        final UUID terminalToken = UUID.randomUUID();
        final String tokenSignature;
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
            .token(terminalToken.toString())
            .agentName(host)
            .agentInternalName(parse.getOptionValue('i', host))
            .agentPort(port)
            .root(lzyRoot);

        if (Files.exists(privateKeyPath)) {
            try (FileReader keyReader = new FileReader(String.valueOf(privateKeyPath))) {
                tokenSignature = Credentials.signToken(terminalToken, keyReader);
                builder.tokenSign(tokenSignature);
            }
        }
        final LzyAgent terminal = new LzyTerminal(builder.build());

        terminal.start();
        terminal.awaitTermination();
        return 0;
    }
}
