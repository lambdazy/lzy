package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Credentials;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;

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

public class Terminal implements ServantCommand {
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
        final LzyServant.Builder builder = LzyServant.Builder.forLzyServer(URI.create(serverAddress))
            .user(System.getenv("USER"))
            .token(terminalToken.toString())
            .servantName(host)
            .servantInternalName(parse.getOptionValue('i', host))
            .servantPort(port)
            .root(lzyRoot)
            .isTerminal(true);

        if (Files.exists(privateKeyPath)) {
            tokenSignature = Credentials.signToken(terminalToken, new String(Files.readAllBytes(privateKeyPath)));
            builder.tokenSign(tokenSignature);
        }
        final LzyServant servant = builder.build();

        servant.start();
        servant.awaitTermination();
        return 0;
    }
}
