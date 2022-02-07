package ru.yandex.cloud.ml.platform.lzy.server.utils.azure;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AmazonCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureSASCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.server.storage.AzureSASCredentialsImpl;

public class StorageUtils {

    public static StorageCredentials.AzureSASCredentials getCredentialsByBucket(String uid, String bucket, StorageConfigs.AzureCredentials credentials){
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

        URI endpointUri = URI.create(String.format("https://%s.blob.core.windows.net?%s",client.getAccountName(), blobClient.generateSas(builder)));

        return new AzureSASCredentialsImpl(
            getQueryMap(endpointUri.getQuery()).get("sig"),
            endpointUri.toString()
        );
    }


    public static void createBucketIfNotExists(StorageCredentials credentials, String bucket){
        switch (credentials.type()){
            case Azure: {
                AzureCredentials azureCredentials = (AzureCredentials) credentials;
                BlobContainerClient client = new BlobServiceClientBuilder()
                    .connectionString(azureCredentials.connectionString())
                    .buildClient()
                    .getBlobContainerClient(bucket);
                if (!client.exists()){
                    client.create();
                }
                return;
            }
            case Amazon: {
                AmazonCredentials amazonCredentials = (AmazonCredentials) credentials;

                String endpoint = amazonCredentials.endpoint();
                if (endpoint.contains("host.docker.internal")) {
                    endpoint = endpoint.replace("host.docker.internal", "localhost");
                }

                AmazonS3 client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        amazonCredentials.accessToken(), amazonCredentials.secretToken()
                    )))
                    .withEndpointConfiguration(
                        new AmazonS3ClientBuilder.EndpointConfiguration(
                           endpoint, "us-west-1"
                        )
                    )
                    .withPathStyleAccessEnabled(true)
                    .build();
                if (!client.doesBucketExistV2(bucket)) {
                    client.createBucket(bucket);
                }
                return;
            }
            case AzureSas: {
                AzureSASCredentials creds = (AzureSASCredentials) credentials;
                BlobContainerClient client = new BlobServiceClientBuilder()
                    .endpoint(creds.endpoint())
                    .buildClient()
                    .getBlobContainerClient(bucket);
                if (!client.exists()){
                    client.create();
                }
            }
        }
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
