package ru.yandex.cloud.ml.platform.lzy.server.utils;

import com.amazonaws.arn.Arn;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

public class StorageUtils {

    public static Lzy.AzureSASCredentials getCredentialsByBucket(String uid, String bucket, StorageConfigs.AzureCredentials credentials){
        BlobContainerSasPermission blobContainerSasPermission = new BlobContainerSasPermission()
                .setReadPermission(true)
                .setAddPermission(true)
                .setCreatePermission(true)
                .setWritePermission(true)
                .setListPermission(true);
        BlobServiceSasSignatureValues builder = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusDays(1), blobContainerSasPermission)
                .setProtocol(SasProtocol.HTTPS_HTTP);
        BlobServiceClient client = new BlobServiceClientBuilder()
                .connectionString(credentials.getConnectionString())
                .buildClient();

        BlobContainerClient blobClient = client.getBlobContainerClient(bucket);
        if (!blobClient.exists())
            blobClient.create();

        URI endpointUri = URI.create(String.format("https://%s.blob.core.windows.net?%s",client.getAccountName(), blobClient.generateSas(builder)));

        return Lzy.AzureSASCredentials.newBuilder()
                .setSignature(getQueryMap(endpointUri.getQuery()).get("sig"))
                .setEndpoint(endpointUri.toString())
                .build();
    }

    public static Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<>();

        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }
}
