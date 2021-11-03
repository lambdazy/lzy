package ru.yandex.cloud.ml.platform.lzy.model.utils;

import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;

public class Credentials {
    public static String signToken(UUID token, Reader privateKeyReader) throws
            InvalidKeySpecException,
            NoSuchAlgorithmException,
            InvalidKeyException,
            SignatureException,
            IOException {
        java.security.Security.addProvider(
                new org.bouncycastle.jce.provider.BouncyCastleProvider()
        );
        KeyFactory factory = KeyFactory.getInstance("RSA");
        try (PemReader pemReader = new PemReader(privateKeyReader)) {
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(content);
            PrivateKey privateKey = factory.generatePrivate(privateKeySpec);

            final String tokenSignature;
            final Signature sign = Signature.getInstance("SHA1withRSA");
            sign.initSign(privateKey);
            sign.update(token.toString().getBytes());
            tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
            return tokenSignature;
        }
    }
}
