package ru.yandex.cloud.ml.platform.lzy.model.utils;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class SignTokenTest {
    @Test
    public void test() throws Exception {
        final Path tempDirectory = Files.createTempDirectory("test-rsa-keys");
        final Path publicKeyPath = tempDirectory.resolve("public.pem");
        final Path privateKeyPath = tempDirectory.resolve("private.pem");

        final Process exec = Runtime.getRuntime()
            .exec(String.format("openssl genrsa -out %s 2048", privateKeyPath));
        exec.waitFor();
        final Process exec1 = Runtime.getRuntime()
            .exec(String.format("openssl rsa -in %s -outform PEM -pubout -out %s", privateKeyPath, publicKeyPath));
        exec1.waitFor();

        final UUID terminalToken = UUID.randomUUID();
        try (FileReader keyReader = new FileReader(privateKeyPath.toFile())) {
            String tokenSignature = Credentials.signToken(terminalToken, keyReader);
            try (FileReader reader = new FileReader(publicKeyPath.toFile())) {
                Assert.assertTrue(Credentials.checkToken(reader, terminalToken.toString(), tokenSignature));
            }
        }
    }
}
