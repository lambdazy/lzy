package ai.lzy.util.auth.credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class RsaUtils {

    public record RsaKeys(
        String publicKey,
        String privateKey
    ) {}

    public static RsaKeys generateRsaKeys() throws IOException, InterruptedException {
        final Path tempDirectory = Files.createTempDirectory("gen-rsa-keys");
        final Path publicKeyPath = tempDirectory.resolve("public.pem");
        final Path privateKeyPath = tempDirectory.resolve("private.pem");

        try {
            var exec = Runtime.getRuntime()
                .exec("openssl genrsa -out %s 2048".formatted(privateKeyPath));
            exec.waitFor();

            var exec1 = Runtime.getRuntime()
                .exec("openssl rsa -in %s -outform PEM -pubout -out %s".formatted(privateKeyPath, publicKeyPath));
            exec1.waitFor();

            return new RsaKeys(Files.readString(publicKeyPath), Files.readString(privateKeyPath));
        } finally {
            Files.delete(publicKeyPath);
            Files.delete(privateKeyPath);
            Files.delete(tempDirectory);
        }
    }
}
