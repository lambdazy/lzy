package ai.lzy.servant.portal;

import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.slots.S3StorageInputSlot;
import ai.lzy.servant.portal.slots.S3StorageOutputSlot;
import ai.lzy.servant.portal.s3.*;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.protobuf.ByteString;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

final class ExternalStorage {

    public interface S3RepositoryProvider {
        S3Repository<Stream<ByteString>> get();
    }

    static String DEFAULT_TRANSMITTER_NAME = "transmitter";
    static int DEFAULT_DOWNLOAD_POOL_SIZE = 10;
    static int DEFAULT_UPLOAD_POOL_SIZE = 10;

    private final Map<S3RepositoryProvider, S3Repository<Stream<ByteString>>> repositories
        = new HashMap<>();
    private final Map<S3RepositoryProvider, S3StorageOutputSlot> toLoad = new HashMap<>();
    private final Map<S3RepositoryProvider, S3StorageInputSlot> toStore = new HashMap<>();

    private final Map<String, S3RepositoryProvider> slotToS3Key = new HashMap<>(); // slotName -> s3Key

    S3StorageOutputSlot getOutputSlot(String slotName) {
        return toLoad.get(slotToS3Key.get(slotName));
    }

    S3StorageInputSlot getInputSlot(String slotName) {
        return toStore.get(slotToS3Key.get(slotName));
    }

    Collection<S3StorageInputSlot> getInputSlots() {
        return toStore.values();
    }

    Collection<S3StorageOutputSlot> getOutputSlots() {
        return toLoad.values();
    }

    void removeInputSlot(String slotName) {
        S3RepositoryProvider s3Key = slotToS3Key.get(slotName);
        toStore.remove(s3Key);
        if (!toLoad.containsKey(s3Key)) {
            slotToS3Key.remove(slotName);
        }
    }

    void removeOutputSlot(String slotName) {
        S3RepositoryProvider s3Key = slotToS3Key.get(slotName);
        toLoad.remove(s3Key);
        if (!toStore.containsKey(s3Key)) {
            slotToS3Key.remove(slotName);
        }
    }

    S3StorageInputSlot createSlotSnapshot(SlotInstance instance, String key,
                                          String bucket, S3RepositoryProvider s3key) {
        return toStore.computeIfAbsent(s3key, k -> {
            slotToS3Key.put(instance.name(), s3key);
            return new S3StorageInputSlot(instance, key, bucket,
                    repositories.computeIfAbsent(k, S3RepositoryProvider::get));
        });
    }

    S3StorageOutputSlot readSlotSnapshot(SlotInstance instance, String key, String bucket, S3RepositoryProvider s3key) {
        return toLoad.computeIfAbsent(s3key, k -> {
            slotToS3Key.put(instance.name(), s3key);
            return new S3StorageOutputSlot(instance, key, bucket,
                    repositories.computeIfAbsent(k, S3RepositoryProvider::get));
        });
    }

    record AmazonS3Key(String endpoint, String accessToken, String secretToken) implements S3RepositoryProvider {
        static AmazonS3Key of(String endpoint, String accessToken, String secretToken) {
            return new AmazonS3Key(endpoint, accessToken, secretToken);
        }

        @Override
        public S3Repository<Stream<ByteString>> get() {
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessToken, secretToken);
            AmazonS3 client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(
                            new AmazonS3ClientBuilder.EndpointConfiguration(
                                    endpoint, "us-west-1"
                            )
                    )
                    .withPathStyleAccessEnabled(true)
                    .build();
            Transmitter transmitter = new AmazonTransmitterFactory(client).fixedPoolsTransmitter(
                    DEFAULT_TRANSMITTER_NAME, DEFAULT_DOWNLOAD_POOL_SIZE, DEFAULT_UPLOAD_POOL_SIZE
            );
            return new AmazonS3RepositoryAdapter<>(client, transmitter, DEFAULT_DOWNLOAD_POOL_SIZE,
                    new ByteStringStreamConverter());
        }
    }

    record AzureS3Key(String connectionString) implements S3RepositoryProvider {
        static AzureS3Key of(String connectionString) {
            return new AzureS3Key(connectionString);
        }

        @Override
        public S3Repository<Stream<ByteString>> get() {
            return null;
        }
    }
}
