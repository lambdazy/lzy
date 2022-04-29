package ru.yandex.cloud.ml.platform.lzy.slots;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import ru.yandex.cloud.ml.platform.lzy.model.UriScheme;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.snapshot.Snapshooter;
import ru.yandex.cloud.ml.platform.lzy.snapshot.SnapshooterImpl;
import ru.yandex.cloud.ml.platform.lzy.storage.StorageClient;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SlotConnectionManager {
    private final Map<String, Transmitter> transmitters = new HashMap<>();
    private final Snapshooter snapshooter;

    public SlotConnectionManager(Lzy.GetS3CredentialsResponse credentials, IAM.Auth auth, URI wb, String bucket,
                                 String sessionId) {
        final StorageClient client = StorageClient.create(credentials);
        final String endpoint;
        if (credentials.hasAmazon()) {
            endpoint = URI.create(credentials.getAmazon().getEndpoint()).getHost();
        } else if (credentials.hasAzure()) {
            endpoint = URI.create(credentials.getAzure().getConnectionString()).getHost();
        } else if (credentials.hasAzureSas()) {
            endpoint = URI.create(credentials.getAzureSas().getEndpoint()).getHost();
        } else {
            throw new RuntimeException("Cannot init ConnectionManager from credentials");
        }

        transmitters.put(endpoint, client.transmitter());
        if (wb != null) {
            final ManagedChannel channelWb = ChannelBuilder
                .forAddress(wb.getHost(), wb.getPort())
                .usePlaintext()
                .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
                .build();
            final SnapshotApiGrpc.SnapshotApiBlockingStub api = SnapshotApiGrpc.newBlockingStub(channelWb);
            this.snapshooter = new SnapshooterImpl(auth, bucket, api, client, sessionId);
        } else {
            this.snapshooter = null;
        }
    }

    public Stream<ByteString> connectToSlot(URI slotUri, long offset) {
        final Iterator<Servant.Message> msgIter;
        final ManagedChannel channel;

        if (UriScheme.LzyKharon.match(slotUri)) {
            channel = ChannelBuilder
                .forAddress(slotUri.getHost(), slotUri.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
            final LzyKharonGrpc.LzyKharonBlockingStub stub = LzyKharonGrpc.newBlockingStub(channel);

            msgIter = new LazyIterator<>(() -> stub.openOutputSlot(Servant.SlotRequest.newBuilder()
                .setOffset(offset)
                .setSlotUri(slotUri.toString())
                .build()
            ));
        } else  {
            channel = ChannelBuilder
                .forAddress(slotUri.getHost(), slotUri.getPort())
                .usePlaintext()
                .enableRetry(LzyFsGrpc.SERVICE_NAME)
                .build();
            final LzyFsGrpc.LzyFsBlockingStub stub = LzyFsGrpc.newBlockingStub(channel);

            msgIter = new LazyIterator<>(() -> stub.openOutputSlot(Servant.SlotRequest.newBuilder()
                .setOffset(offset)
                .setSlotUri(slotUri.toString())
                .build()
            ));
        }

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(msgIter, Spliterator.NONNULL), false)
            .map(msg -> msg.hasChunk() ? msg.getChunk() : ByteString.EMPTY)
            .onClose(channel::shutdownNow);
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
                        while (!queue.offer(chunk, 1, TimeUnit.SECONDS)) ;
                        len = stream.read(buffer);
                    }
                    //noinspection StatementWithEmptyBody,CheckStyle
                    while (!queue.offer(ByteString.EMPTY, 1, TimeUnit.SECONDS)) ;
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

    private static class LazyIterator<T> implements Iterator<T> {
        private final Supplier<Iterator<T>> supplier;
        private Iterator<T> createdIterator;

        public LazyIterator(Supplier<Iterator<T>> supplier) {
            this.supplier = supplier;
        }

        @Override
        public boolean hasNext() {
            return iter().hasNext();
        }

        @Override
        public T next() {
            return iter().next();
        }

        private synchronized Iterator<T> iter() {
            if (createdIterator == null) {
                createdIterator = supplier.get();
            }
            return createdIterator;
        }
    }
}
