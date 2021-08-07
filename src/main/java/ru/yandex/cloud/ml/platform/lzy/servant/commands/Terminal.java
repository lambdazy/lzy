package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
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
            throw new RuntimeException("Provide lzy server address with -a option to start a task.");
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
        tokenSignature = signToken(terminalToken, privateKeyPath);

        final LzyServant servant = LzyServant.Builder.forLzyServer(URI.create(serverAddress))
            .user(System.getenv("USER"))
            .token(terminalToken.toString())
            .tokenSign(tokenSignature)
            .servantName(parse.getOptionValue('a', LzyFS.lineCmd("hostname")))
            .servantPort(port)
            .root(Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy")))
            .build();

        servant.start();
        servant.awaitTermination();
        return 0;
    }

    private static String signToken(UUID terminalToken, Path privateKeyPath) throws
                                                                             IOException,
                                                                             InvalidKeySpecException,
                                                                             NoSuchAlgorithmException,
                                                                             InvalidKeyException,
                                                                             SignatureException {
        java.security.Security.addProvider(
            new org.bouncycastle.jce.provider.BouncyCastleProvider()
        );

        final String tokenSignature;
        final byte[] privKeyPEM = Base64.getDecoder().decode(
            new String(Files.readAllBytes(privateKeyPath))
                .replaceAll("-----[^-]*-----\\n", "")
                .replaceAll("\\R", "")
        );

        final PrivateKey rsaKey = KeyFactory.getInstance("RSA")
            .generatePrivate(new PKCS8EncodedKeySpec(privKeyPEM));
        final Signature sign = Signature.getInstance("SHA1withRSA");
        sign.initSign(rsaKey);
        sign.update(terminalToken.toString().getBytes());
        tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
        return tokenSignature;
    }
}
