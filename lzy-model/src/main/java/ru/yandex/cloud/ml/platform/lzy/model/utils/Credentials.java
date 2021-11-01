package ru.yandex.cloud.ml.platform.lzy.model.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;

public class Credentials {
    public static String signToken(UUID token, String privateKey) throws
            InvalidKeySpecException,
            NoSuchAlgorithmException,
            InvalidKeyException,
            SignatureException {
        java.security.Security.addProvider(
                new org.bouncycastle.jce.provider.BouncyCastleProvider()
        );

        final String tokenSignature;
        final byte[] privKeyPEM = Base64.getDecoder().decode(
                privateKey
                        .replaceAll("-----[^-]*-----\\n", "")
                        .replaceAll("\\R", "")
        );

        final PrivateKey rsaKey = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(privKeyPEM));
        final Signature sign = Signature.getInstance("SHA1withRSA");
        sign.initSign(rsaKey);
        sign.update(token.toString().getBytes());
        tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
        return tokenSignature;
    }
}
