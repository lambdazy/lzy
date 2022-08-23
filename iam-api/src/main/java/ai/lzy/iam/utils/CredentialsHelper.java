package ai.lzy.iam.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.Map;

import static java.security.Security.addProvider;

public class CredentialsHelper {

    public static boolean checkJWT(Reader keyReader, String jwt, String uid)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (PemReader pemReader = new PemReader(keyReader)) {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PemObject pemObject = pemReader.readPemObject();
            if (pemObject == null) {
                return false;
            }
            byte[] content = pemObject.getContent();
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(content);
            PublicKey rsaKey = factory.generatePublic(pubKeySpec);
            return checkJWT(rsaKey, jwt, uid);
        }
    }

    public static boolean checkJWT(PublicKey key, String jwt, String uid) {
        try {
            Jwts.parserBuilder()
                    .requireIssuer(uid)
                    .setSigningKey(key)
                    .build()
                    .parseClaimsJws(jwt);
            return true;
        } catch (JwtException ex) {
            return false;
        }
    }

    public static String issuerFromJWT(String jwt) {
        String[] chunks = jwt.split("\\.");
        Base64.Decoder decoder = Base64.getUrlDecoder();
        String payload = new String(decoder.decode(chunks[1]));
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            var map = objectMapper.readValue(payload, Map.class);
            return (String) map.get("iss");
        } catch (JsonProcessingException ignored) {
            return null;
        }
    }

    public static String buildJWT(String uid, String privateKey)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        return buildJWT(uid, new StringReader(privateKey));
    }

    public static String buildJWT(String uid, Reader privateKeyReader)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        addProvider(new BouncyCastleProvider());
        KeyFactory factory = KeyFactory.getInstance("RSA");
        try (PemReader pemReader = new PemReader(privateKeyReader)) {
            PemObject pemObject = pemReader.readPemObject();
            byte[] content = pemObject.getContent();
            PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(content);
            PrivateKey privateKey = factory.generatePrivate(privateKeySpec);
            Instant now = Instant.now();
            return Jwts.builder()
                    .setIssuedAt(Date.from(now))
                    .setNotBefore(Date.from(now))
                    .setExpiration(Date.from(now.plusSeconds(Duration.ofDays(7).toSeconds())))
                    .setIssuer(uid)
                    .signWith(privateKey, SignatureAlgorithm.PS256)
                    .compact();
        }
    }
}
