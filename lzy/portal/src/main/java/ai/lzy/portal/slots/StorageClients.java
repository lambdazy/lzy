package ai.lzy.portal.slots;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientWithTransmitter;
import ai.lzy.storage.azure.AzureClientWithTransmitter;
import ai.lzy.storage.s3.S3ClientWithTransmitter;
import ai.lzy.v1.common.LMST;

import java.util.concurrent.ExecutorService;

public class StorageClients {
    public interface Provider<T extends StorageClient> {
        T get(ExecutorService transmitterExecutor);
    }

    record S3Provider(String endpoint, String accessToken, String secretToken)
        implements Provider<S3ClientWithTransmitter>
    {
        @Override
        public S3ClientWithTransmitter get(ExecutorService transmitterExecutor) {
            return new S3ClientWithTransmitter(endpoint, accessToken, secretToken, transmitterExecutor);
        }
    }

    record AzureProvider(String connectionString) implements Provider<AzureClientWithTransmitter> {
        @Override
        public AzureClientWithTransmitter get(ExecutorService transmitterExecutor) {
            return new AzureClientWithTransmitter(connectionString, transmitterExecutor);
        }
    }

    public static Provider<? extends StorageClientWithTransmitter> provider(LMST.StorageConfig storageConfig) {
        if (storageConfig.hasAzure()) {
            return new AzureProvider(storageConfig.getAzure().getConnectionString());
        } else {
            assert storageConfig.hasS3();
            return new S3Provider(storageConfig.getS3().getEndpoint(), storageConfig.getS3().getAccessToken(),
                storageConfig.getS3().getSecretToken());
        }
    }
}
