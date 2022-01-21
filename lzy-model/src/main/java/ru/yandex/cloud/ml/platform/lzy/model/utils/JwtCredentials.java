package ru.yandex.cloud.ml.platform.lzy.model.utils;

import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.Reader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.Date;

import static java.security.Security.addProvider;

public class JwtCredentials {
    public static String buildJWT(String uid, PrivateKey key){
        Instant now = Instant.now();
        return Jwts.builder()
            .setIssuedAt(Date.from(now))
            .setNotBefore(Date.from(now))
            .setExpiration(Date.from(now.plusSeconds(60 * 60 * 7)))
            .setIssuer(uid)
            .signWith(key, SignatureAlgorithm.PS256)
            .compact();
    }

    public static String buildJWT(String uid, Reader privateKeyReader) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        addProvider(new BouncyCastleProvider());
        KeyFactory factory = KeyFactory.getInstance("RSA");
        try (PemReader pemReader = new PemReader(privateKeyReader)) {
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(content);
            PrivateKey privateKey = factory.generatePrivate(privateKeySpec);

            return buildJWT(uid, privateKey);
        }
    }

    public static boolean checkJWT(PublicKey key, String jwt, String uid){
        try {
            Jwts.parserBuilder()
                .requireIssuer(uid)
                .setSigningKey(key)
                .build()
                .parseClaimsJws(jwt);
            return true;
        }
        catch (JwtException ex){
            return false;
        }
    }

    public static boolean checkJWT(Reader keyReader, String jwt, String uid) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (PemReader pemReader = new PemReader(keyReader)) {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);
            PublicKey rsaKey = factory.generatePublic(pubKeySpec);
            return checkJWT(rsaKey, jwt, uid);
        }
    }
}
