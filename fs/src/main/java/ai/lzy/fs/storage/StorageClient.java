package ai.lzy.fs.storage;

import java.net.URI;
import ru.yandex.qe.s3.transfer.Transmitter;
import ai.lzy.priv.v2.Lzy;

public interface StorageClient {
    String DEFAULT_TRANSMITTER_NAME = "transmitter";
    int DEFAULT_DOWNLOAD_POOL_SIZE = 10;
    int DEFAULT_UPLOAD_POOL_SIZE = 10;

    static StorageClient create(Lzy.GetS3CredentialsResponse credentials, String transmitterName,
                                  int downloadsPoolSize, int chunksPoolSize) {
        if (credentials.hasAmazon()) {
            return new AmazonStorageClient(credentials.getAmazon(), transmitterName, downloadsPoolSize,
                chunksPoolSize);
        } else if (credentials.hasAzure()) {
            return new AzureStorageClient(credentials.getAzure(), transmitterName, downloadsPoolSize, chunksPoolSize);
        } else {
            return new AzureStorageClient(credentials.getAzureSas(), transmitterName, downloadsPoolSize,
                chunksPoolSize);
        }
    }

    static StorageClient create(Lzy.GetS3CredentialsResponse credentials) {
        return create(credentials, DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE);
    }

    static StorageClient createAmazonS3Client(URI endpoint, String accessToken, String secretToken) {
        return new AmazonStorageClient(accessToken, secretToken, endpoint,
                DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE);
    }

    static StorageClient createAzureS3Client(String connectionString) {
        return new AzureStorageClient(connectionString,
                DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE);
    }

    Transmitter transmitter();

    URI getURI(String bucketName, String key);

    String bucket(URI uri);

    String key(URI uri);
}
