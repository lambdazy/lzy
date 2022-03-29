package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.azure.blobstorage.AzureTransmitterFactory;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshooter;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SnapshooterImpl;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc.LzyServerBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class SlotConnectionManager {
    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);
    private final Map<String, Transmitter> transmitters = new HashMap<>();
    private final Snapshooter snapshooter;

    public SlotConnectionManager(LzyServerBlockingStub server, IAM.Auth auth, URI wb, String bucket, String sessionId)
        throws URISyntaxException {
        final Lzy.GetS3CredentialsResponse credentials = server
            .getS3Credentials(
                Lzy.GetS3CredentialsRequest.newBuilder()
                    .setAuth(auth)
                    .setBucket(bucket)
                    .build()
            );
        if (credentials.hasAmazon()) {
            final Lzy.AmazonCredentials amazonCreds = credentials.getAmazon();
            final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                amazonCreds.getAccessToken(),
                amazonCreds.getSecretToken()
            );

            final String endpoint = amazonCreds.getEndpoint();
            final AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withEndpointConfiguration(
                    new AmazonS3ClientBuilder.EndpointConfiguration(
                        endpoint, "us-west-1"
                    )
                )
                .withPathStyleAccessEnabled(true)
                .build();
            transmitters.put(
                new URI(endpoint).getHost(),
                new AmazonTransmitterFactory(client)
                    .fixedPoolsTransmitter(
                        StorageClient.DEFAULT_TRANSMITTER_NAME,
                        StorageClient.DEFAULT_DOWNLOAD_POOL_SIZE,
                        StorageClient.DEFAULT_UPLOAD_POOL_SIZE
                    )
            );
        }
        if (credentials.hasAzure()) {
            final String connectionString = credentials.getAzure().getConnectionString();
            final BlobServiceClient client;
            client = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
            transmitters.put(
                new URI(connectionString).getHost(),
                new AzureTransmitterFactory(client)
                    .fixedPoolsTransmitter(
                        StorageClient.DEFAULT_TRANSMITTER_NAME,
                        StorageClient.DEFAULT_DOWNLOAD_POOL_SIZE,
                        StorageClient.DEFAULT_UPLOAD_POOL_SIZE
                    )
            );
        }
        if (credentials.hasAzureSas()) {
            final String endpoint = credentials.getAzureSas().getEndpoint();
            final BlobServiceClient client;
            client = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .sasToken(credentials.getAzureSas().getSignature())
                .buildClient();
            transmitters.put(
                new URI(endpoint).getHost(),
                new AzureTransmitterFactory(client)
                    .fixedPoolsTransmitter(
                        StorageClient.DEFAULT_TRANSMITTER_NAME,
                        StorageClient.DEFAULT_DOWNLOAD_POOL_SIZE,
                        StorageClient.DEFAULT_UPLOAD_POOL_SIZE
                    )
            );
        }
        if (wb != null) {
            StorageClient storage = StorageClient.create(credentials);
            final ManagedChannel channelWb = ChannelBuilder
                .forAddress(wb.getHost(), wb.getPort())
                .usePlaintext()
                .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
                .build();
            final SnapshotApiGrpc.SnapshotApiBlockingStub api = SnapshotApiGrpc.newBlockingStub(channelWb);
            this.snapshooter = new SnapshooterImpl(auth, bucket, api, storage, sessionId);
        } else {
            this.snapshooter = null;
        }
    }

    public Stream<ByteString> connectToSlot(URI slotUri, long offset) {
        final Channel channel = ChannelBuilder
            .forAddress(slotUri.getHost(), slotUri.getPort())
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub stub = LzyServantGrpc.newBlockingStub(channel);

        final Iterator<Servant.Message> msgIter = stub.openOutputSlot(Servant.SlotRequest.newBuilder()
            .setOffset(offset)
            .setSlotUri(slotUri.toString())
            .build()
        );
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(msgIter, Spliterator.NONNULL),
            false
        ).map(msg -> msg.hasChunk() ? msg.getChunk() : ByteString.EMPTY);
    }

    private Transmitter resolveStorage(URI uri) {
        // TODO: support getting creds for alternative to snapshot target storages
        if (!transmitters.containsKey(uri.getHost())) {
            throw new IllegalArgumentException();
        }
        return transmitters.get(uri.getHost());
    }

    public Stream<ByteString> connectToS3(URI s3Uri, long offset) {
        String storagePath = s3Uri.getPath();
        final String key;
        final String bucket;
        final String[] parts = storagePath.split("/");
        if (storagePath.startsWith("/")) {
            bucket = parts[1];
            key = Arrays.stream(parts).skip(2).collect(Collectors.joining("/"));
        } else {
            bucket = parts[0];
            key = Arrays.stream(parts).skip(1).collect(Collectors.joining("/"));
        }
        final BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(1000);
        resolveStorage(s3Uri).downloadC(
            new DownloadRequestBuilder()
                .bucket(bucket)
                .key(key)
                .build(),
            data -> {
                final byte[] buffer = new byte[4096];
                try (final InputStream stream = data.getInputStream()) {
                    int len = 0;
                    while (len != -1) {
                        final ByteString chunk = ByteString.copyFrom(buffer, 0, len);
                        //noinspection StatementWithEmptyBody,CheckStyle
                        while (!queue.offer(chunk, 1, TimeUnit.SECONDS));
                        len = stream.read(buffer);
                    }
                    //noinspection StatementWithEmptyBody,CheckStyle
                    while (!queue.offer(ByteString.EMPTY, 1, TimeUnit.SECONDS));
                }
            }
        );
        final Iterator<ByteString> chunkIterator = new Iterator<>() {
            ByteString chunk = null;
            @Override
            public boolean hasNext() {
                try {
                    while (chunk == null) {
                        chunk = queue.poll(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                return chunk != ByteString.EMPTY;
            }

            @Override
            public ByteString next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final ByteString chunk = this.chunk;
                this.chunk = null;
                return chunk;
            }
        };
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(chunkIterator, Spliterator.NONNULL),
            false
        );
    }

    public Snapshooter snapshooter() {
        return snapshooter;
    }
}
