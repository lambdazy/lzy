package ai.lzy.storage.util.yc;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.IOException;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.function.Supplier;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class TokenSupplier implements Supplier<String> {
    private final String serviceAccountId;
    private final String keyId;
    private final PrivateKey privateKey;
    private final int retryNum;

    public TokenSupplier(String serviceAccountId, String keyId, PrivateKey privateKey, int retryNum) {
        this.serviceAccountId = serviceAccountId;
        this.keyId = keyId;
        this.privateKey = privateKey;
        this.retryNum = retryNum;
    }

    public TokenSupplier(String serviceAccountId, String keyId, PrivateKey privateKey) {
        this(serviceAccountId, keyId, privateKey, 16);
    }

    private String generateJWT() {

        Instant now = Instant.now();

        return Jwts.builder()
            .setHeaderParam("kid", keyId)
            .setIssuer(serviceAccountId)
            .setAudience("https://iam.api.cloud.yandex.net/iam/v1/tokens")
            .setIssuedAt(Date.from(now))
            .setExpiration(Date.from(now.plusSeconds(360)))
            .signWith(privateKey, SignatureAlgorithm.PS256)
            .compact();
    }

    private String getIAM(CloseableHttpClient httpclient) throws IOException {
        String jwt = generateJWT();
        HttpPost request = new HttpPost("https://iam.api.cloud.yandex.net/iam/v1/tokens/");
        String requestBody = new JSONObject()
            .put("jwt", jwt)
            .toString();
        StringEntity requestEntity = new StringEntity(requestBody);
        request.setEntity(requestEntity);
        CloseableHttpResponse response = httpclient.execute(request);
        JSONObject obj = new JSONObject(EntityUtils.toString(response.getEntity()));
        return obj.getString("iamToken");
    }

    @Override
    public String get() {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        for (int i = 0; i < retryNum; i++) {
            try {
                return getIAM(httpclient);
            } catch (IOException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Cannot get iam token");
    }
}
