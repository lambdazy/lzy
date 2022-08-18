package ai.lzy.util.auth.credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class RsaUtils {
    public record Keys(
        Path publicKeyPath,
        Path privateKeyPath
    ) {}

    public static Keys generateRsaKeys() throws IOException, InterruptedException {
        final Path tempDirectory = Files.createTempDirectory("test-rsa-keys");
        final Path publicKeyPath = tempDirectory.resolve("public.pem");
        final Path privateKeyPath = tempDirectory.resolve("private.pem");

        final Process exec = Runtime.getRuntime()
            .exec(String.format("openssl genrsa -out %s 2048", privateKeyPath));
        exec.waitFor();
        final Process exec1 = Runtime.getRuntime()
            .exec(String.format("openssl rsa -in %s -outform PEM -pubout -out %s", privateKeyPath, publicKeyPath));
        exec1.waitFor();
        return new Keys(publicKeyPath, privateKeyPath);
    }
}
