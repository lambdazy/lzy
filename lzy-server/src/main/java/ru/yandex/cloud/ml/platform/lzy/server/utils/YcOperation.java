package ru.yandex.cloud.ml.platform.lzy.server.utils;

import net.bytebuddy.agent.builder.AgentBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;

public class YcOperation {
    public static JSONObject getResult(long retryInterval, int retryNum, String operationId, CloseableHttpClient client, String iam) throws InterruptedException, IOException {
        for (int i = 0; i < retryNum; i++){
            HttpGet request = new HttpGet("https://operation.api.cloud.yandex.net/operations/" + operationId);
            request.setHeader("Authorization", "Bearer " + iam);
            CloseableHttpResponse response = client.execute(request);
            JSONObject obj = new JSONObject(EntityUtils.toString(response.getEntity()));
            if (obj.getBoolean("done")){
                if (obj.has("error")){
                    throw new RuntimeException("Error while executing operation: " + obj.getJSONObject("error").toString());
                }
                return obj.getJSONObject("response");
            }
            Thread.sleep(retryInterval);
        }
        throw new RuntimeException("Retry num exceeded");
    }

    public static JSONObject getResult(String operationId, CloseableHttpClient client, String iam) throws InterruptedException, IOException{
        return getResult(1000, 16, operationId, client, iam);
    }
}
