package ru.yandex.cloud.ml.platform.lzy.iam.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import java.io.IOException;
import java.io.Reader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;

public class CredentialsHelper {

    public static boolean checkJWT(Reader keyReader, String jwt, String uid)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (PemReader pemReader = new PemReader(keyReader)) {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PemObject pemObject = pemReader.readPemObject();
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
}
