package ai.lzy.storage.util.yc;

import ai.lzy.storage.StorageConfig;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

@Context  //To make static initialization
@Requires(property = "storage.yc.enabled", value = "true")
public class RenewableToken {
    private static final AtomicReference<RenewableTokenInstance> instance = new AtomicReference<>();

    public RenewableToken(StorageConfig.YcCredentials yc) {
        assert yc.enabled();

        PemObject privateKeyPem;
        try (PemReader reader = new PemReader(new StringReader(yc.privateKey()))) {
            privateKeyPem = reader.readPemObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        PrivateKey privateKey;
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyPem.getContent()));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        TokenSupplier supplier = new TokenSupplier(
            yc.serviceAccountId(),
            yc.keyId(),
            privateKey
        );

        instance.set(new RenewableTokenInstance(supplier));
    }

    public static String getToken() {
        return Objects.requireNonNull(instance.get()).get();
    }
}
