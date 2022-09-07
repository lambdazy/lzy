package ai.lzy.util.auth.credentials;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import static java.security.Security.addProvider;

public enum CredentialsUtils {
    ;

    public static String signToken(String token, Reader privateKeyReader)
        throws InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, IOException
    {
        var privateKey = readPrivateKey(privateKeyReader);
        return signToken(token, privateKey);
    }

    public static String signToken(String token, PrivateKey privateKey)
        throws NoSuchAlgorithmException, InvalidKeyException, SignatureException
    {
        addProvider(new BouncyCastleProvider());

        final String tokenSignature;
        final Signature sign = Signature.getInstance("SHA1withRSA");
        sign.initSign(privateKey);
        sign.update(token.getBytes());
        tokenSignature = new String(Base64.getEncoder().encode(sign.sign()));
        return tokenSignature;
    }

    public static boolean checkToken(PublicKey publicKey, String token, String tokenSign)
        throws InvalidKeyException, SignatureException
    {
        try {
            final Signature sign = Signature.getInstance("SHA1withRSA");
            sign.initVerify(publicKey);
            sign.update(token.getBytes());

            if (sign.verify(Base64.getDecoder().decode(tokenSign))) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
    }

    public static boolean checkToken(Reader keyReader, String token, String tokenSign)
        throws IOException, InvalidKeyException, SignatureException, InvalidKeySpecException
    {
        PublicKey publicKey;
        try {
            publicKey = readPublicKey(keyReader);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(System.err);
            return false;
        }

        return checkToken(publicKey, token, tokenSign);
    }

    public static boolean checkToken(Path keyPath, String token, String tokenSign)
        throws IOException, InvalidKeyException, SignatureException, InvalidKeySpecException
    {
        try (var reader = new FileReader(keyPath.toFile())) {
            return checkToken(reader, token, tokenSign);
        }
    }

    public static PublicKey readPublicKey(Path filePath)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        try (var reader = new FileReader(filePath.toFile())) {
            return readPublicKey(reader);
        }
    }

    public static PublicKey readPublicKey(String key)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        try (var reader = new StringReader(key)) {
            return readPublicKey(reader);
        }
    }

    public static PublicKey readPublicKey(Reader keyReader)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        try (PemReader pemReader = new PemReader(keyReader)) {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);
            return factory.generatePublic(pubKeySpec);
        }
    }

    public static PrivateKey readPrivateKey(Path filePath)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        try (var reader = new FileReader(filePath.toFile())) {
            return readPrivateKey(reader);
        }
    }

    public static PrivateKey readPrivateKey(String key)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        try (var reader = new StringReader(key)) {
            return readPrivateKey(reader);
        }
    }

    public static PrivateKey readPrivateKey(Reader privateKeyReader)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        addProvider(new BouncyCastleProvider());
        KeyFactory factory = KeyFactory.getInstance("RSA");

        try (PemReader pemReader = new PemReader(privateKeyReader)) {
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(content);
            return factory.generatePrivate(privateKeySpec);
        }
    }
}
