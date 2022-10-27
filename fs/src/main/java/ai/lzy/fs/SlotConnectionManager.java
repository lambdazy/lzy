package ai.lzy.fs;

import ai.lzy.fs.snapshot.SlotSnapshotImpl;
import ai.lzy.fs.snapshot.SlotSnapshotProvider;
import ai.lzy.fs.snapshot.Snapshooter;
import ai.lzy.fs.snapshot.SnapshooterImpl;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.deprecated.Lzy;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import ai.lzy.v1.deprecated.SnapshotApiGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;

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
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class SlotConnectionManager {
    private final Map<String, Transmitter> transmitters = new HashMap<>();
    private final Snapshooter snapshooter;

    @Deprecated
    public SlotConnectionManager(@Nullable Lzy.GetS3CredentialsResponse credentials,
                                 LzyAuth.Auth auth, @Nullable URI wb, String bucket)
    {
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
            final var channelWb = newGrpcChannel(wb.getHost(), wb.getPort(), SnapshotApiGrpc.SERVICE_NAME);
            final SnapshotApiGrpc.SnapshotApiBlockingStub api = SnapshotApiGrpc.newBlockingStub(channelWb);
            this.snapshooter = new SnapshooterImpl(auth, api, new SlotSnapshotProvider.Cached(slot ->
                new SlotSnapshotImpl(bucket, slot, client)
            ));
        } else {
            this.snapshooter = null;
        }
    }

    public SlotConnectionManager() {
        snapshooter = null;
    }

    public static Stream<ByteString> connectToSlot(SlotInstance slotInstance, long offset) {
        final Iterator<LzyFsApi.Message> msgIter;
        final ManagedChannel channel;

        final URI slotUri = slotInstance.uri();
        final LzyFsApi.SlotRequest request = LzyFsApi.SlotRequest.newBuilder()
            .setOffset(offset)
            .setSlotInstance(ProtoConverter.toProto(slotInstance))
            .build();

        if (UriScheme.LzyKharon.match(slotUri)) {
            channel = newGrpcChannel(slotUri.getHost(), slotUri.getPort(), LzyKharonGrpc.SERVICE_NAME);
            final LzyKharonGrpc.LzyKharonBlockingStub stub = LzyKharonGrpc.newBlockingStub(channel);

            msgIter = new LazyIterator<>(() -> stub.openOutputSlot(request));
        } else if (UriScheme.LzyFs.match(slotUri)) {
            channel = newGrpcChannel(slotUri.getHost(), slotUri.getPort(), LzyFsGrpc.SERVICE_NAME);
            final LzyFsGrpc.LzyFsBlockingStub stub = LzyFsGrpc.newBlockingStub(channel);

            msgIter = new LazyIterator<>(() -> stub.openOutputSlot(request));
        } else {
            throw new RuntimeException("Unexpected slot type: " + slotUri);
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
        assert UriScheme.SlotS3.match(s3Uri) || UriScheme.SlotAzure.match(s3Uri) : s3Uri.toString();

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
                        while (!queue.offer(chunk, 1, TimeUnit.SECONDS)) {}
                        len = stream.read(buffer);
                    }
                    //noinspection StatementWithEmptyBody,CheckStyle
                    while (!queue.offer(ByteString.EMPTY, 1, TimeUnit.SECONDS)) {}
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
