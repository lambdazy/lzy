package ai.lzy.util.auth.credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class RsaUtils {

    public record RsaKeysFiles(
        Path publicKeyPath,
        Path privateKeyPath
    ) {}

    public static RsaKeysFiles generateRsaKeysFiles() throws IOException, InterruptedException {
        final Path tempDirectory = Files.createTempDirectory("test-rsa-keys");
        final Path publicKeyPath = tempDirectory.resolve("public.pem");
        final Path privateKeyPath = tempDirectory.resolve("private.pem");

        final Process exec = Runtime.getRuntime()
            .exec(String.format("openssl genrsa -out %s 2048", privateKeyPath));
        exec.waitFor();
        final Process exec1 = Runtime.getRuntime()
            .exec(String.format("openssl rsa -in %s -outform PEM -pubout -out %s", privateKeyPath, publicKeyPath));
        exec1.waitFor();
        return new RsaKeysFiles(publicKeyPath, privateKeyPath);
    }

    public record RsaKeys(
        String publicKey,
        String privateKey
    ) {}

    public static RsaKeys generateRsaKeys() throws IOException, InterruptedException {
        var files = generateRsaKeysFiles();

        var keys = new RsaKeys(Files.readString(files.publicKeyPath()), Files.readString(files.privateKeyPath()));
        Files.delete(files.publicKeyPath());
        Files.delete(files.privateKeyPath());
        return keys;
    }
}
