package ru.yandex.cloud.ml.platform.lzy.server.utils.yc;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;

@Context  //To make static initialization
@Requires(property = "server.yc.enabled", value = "true")
public class RenewableToken{
    private static RenewableTokenInstance instance;

    public RenewableToken(ServerConfig serverConfig){
        PemObject privateKeyPem;
        try (PemReader reader = new PemReader(new StringReader(serverConfig.getYc().getPrivateKey()))) {
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
            serverConfig.getYc().getServiceAccountId(),
            serverConfig.getYc().getKeyId(),
            privateKey
        );

        instance = new RenewableTokenInstance(supplier);
    }


    public static String getToken() {
        synchronized (instance) {
            return instance.get();
        }
    }
}
