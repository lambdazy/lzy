package ai.lzy.model.utils;

import static java.security.Security.addProvider;

import java.io.IOException;
import java.io.Reader;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.UUID;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

public class Credentials {
    public static String signToken(UUID token, Reader privateKeyReader) throws
        InvalidKeySpecException,
        NoSuchAlgorithmException,
        InvalidKeyException,
        SignatureException,
        IOException {
        addProvider(new BouncyCastleProvider());
        KeyFactory factory = KeyFactory.getInstance("RSA");
        try (PemReader pemReader = new PemReader(privateKeyReader)) {
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(content);
            PrivateKey privateKey = factory.generatePrivate(privateKeySpec);

            return signToken(token, privateKey);
        }
    }

    public static String signToken(UUID token, PrivateKey privateKey)
        throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        java.security.Security.addProvider(
            new org.bouncycastle.jce.provider.BouncyCastleProvider()
        );

        final String tokenSignature;
        final Signature sign = Signature.getInstance("SHA1withRSA");
        sign.initSign(privateKey);
        sign.update(token.toString().getBytes());
        tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
        return tokenSignature;
    }

    public static boolean checkToken(Reader keyReader, String token, String tokenSign)
        throws IOException, InvalidKeyException, SignatureException, InvalidKeySpecException {
        try (PemReader pemReader = new PemReader(keyReader)) {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);
            PublicKey rsaKey = factory.generatePublic(pubKeySpec);

            final Signature sign = Signature.getInstance("SHA1withRSA");
            sign.initVerify(rsaKey);
            sign.update(token.getBytes());

            if (sign.verify(Base64.getDecoder().decode(tokenSign))) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }
}
