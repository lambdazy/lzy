package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.channel.local.LocalEventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;
import java.util.UUID;

@Singleton
@Requires(property = "server.uri")
public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {
    private final SnapshotRepository repository;
    private final static Logger LOG = LogManager.getLogger(SnapshotApi.class);
    private final Authenticator auth;
    private final SnapshotRepository repository;

    @Inject
    public SnapshotApi(ServerConfig serverConfig, SnapshotRepository repository) {
        URI uri = URI.create(serverConfig.getUri());
        final ManagedChannel serverChannel = ManagedChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
        auth = new SimpleAuthenticator(LzyServerGrpc.newBlockingStub(serverChannel));
        this.repository = repository;
    }

    @Override
    public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request, StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
        if (!auth.checkPermissions(request.getAuth())) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        URI snapshotId = URI.create(UUID.randomUUID().toString());
        URI ownerId = URI.create(request.getAuth().getUser().getUserId());
        repository.create(new Snapshot.Impl(snapshotId, ownerId));
        final LzyWhiteboard.Snapshot result = LzyWhiteboard.Snapshot
                .newBuilder()
                .setSnapshotId(snapshotId.toString())
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void prepareToSave(LzyWhiteboard.PrepareCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        if (!auth.checkPermissions(request.getAuth())) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        repository.prepare(gRPCConverter.from(request.getEntry(), snapshotStatus.snapshot()),
                request.getEntry().getDependentEntryIdsList());
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(LzyWhiteboard.CommitCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        if (!auth.checkPermissions(request.getAuth())) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        final SnapshotEntry entry = repository.resolveEntry(snapshotStatus.snapshot(), request.getEntryId());
        if (entry == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        repository.commit(entry, request.getEmpty());
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        if (!auth.checkPermissions(request.getAuth())) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        repository.finalize(snapshotStatus.snapshot());
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }
}
