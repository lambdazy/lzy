package ai.lzy.util.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;

public class YcIamClient {
    public static String createServiceAccount(String uid, String iamToken, CloseableHttpClient httpclient,
        String folderId, String bucket) throws IOException, InterruptedException {
        HttpPost request = new HttpPost("https://iam.api.cloud.yandex.net/iam/v1/serviceAccounts/");
        String requestBody = new JSONObject()
            .put("folderId", folderId)
            .put("name", bucket)
            .put("description", "service account for user " + uid)
            .toString();
        String opId = executeRequest(iamToken, httpclient, request, requestBody).getString("id");
        JSONObject obj = YcOperation.getResult(opId, httpclient, iamToken);
        return obj.getString("id");
    }

    public static JSONObject executeRequest(String iamToken, CloseableHttpClient httpclient, HttpPost request,
        String requestBody) throws IOException {
        StringEntity requestEntity = new StringEntity(requestBody);
        request.setEntity(requestEntity);
        request.setHeader("Content-type", "application/json");
        request.setHeader("Authorization", "Bearer " + iamToken);
        CloseableHttpResponse response = httpclient.execute(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException(String.format("Code: %s, reason: %s", response.getStatusLine().getStatusCode(),
                response.getStatusLine().getReasonPhrase()));
        }
        String body = EntityUtils.toString(response.getEntity());
        return new JSONObject(body);
    }

    public static AWSCredentials createStaticCredentials(String serviceAccountId, String iamToken,
        CloseableHttpClient httpclient) throws IOException {
        HttpPost httppost = new HttpPost("https://iam.api.cloud.yandex.net/iam/aws-compatibility/v1/accessKeys/");
        String requestBody = new JSONObject()
            .put("serviceAccountId", serviceAccountId)
            .put("description", "key for service account id " + serviceAccountId)
            .toString();
        JSONObject credentials = executeRequest(iamToken, httpclient, httppost, requestBody);

        String accessKeyId = credentials.getJSONObject("accessKey").getString("keyId");
        String secretKey = credentials.getString("secret");
        return new BasicAWSCredentials(accessKeyId, secretKey);
    }
}
