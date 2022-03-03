package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

@Singleton
@Requires(property = "server.uri")
public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {

    private static final Logger LOG = LogManager.getLogger(SnapshotApi.class);
    private final Authenticator auth;
    private final SnapshotRepository repository;

    @Inject
    public SnapshotApi(ServerConfig serverConfig, SnapshotRepository repository) {
        URI uri = URI.create(serverConfig.getUri());
        final ManagedChannel serverChannel = ChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        auth = new SimpleAuthenticator(LzyServerGrpc.newBlockingStub(serverChannel),
            LzyBackofficeGrpc.newBlockingStub(serverChannel));
        this.repository = repository;
    }

    @Override
    public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request,
                               StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
        LOG.info("SnapshotApi::createSnapshot " + JsonUtils.printRequest(request));
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        URI snapshotId = URI.create(UUID.randomUUID().toString());
        repository.create(new Snapshot.Impl(snapshotId, URI.create(request.getAuth().getUser().getUserId())));
        final LzyWhiteboard.Snapshot result = LzyWhiteboard.Snapshot
            .newBuilder()
            .setSnapshotId(snapshotId.toString())
            .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void prepareToSave(LzyWhiteboard.PrepareCommand request,
                              StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::prepareToSave " + JsonUtils.printRequest(request));
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository
            .resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        repository.prepare(GrpcConverter.from(request.getEntry(), snapshotStatus.snapshot()),
            request.getEntry().getStorageUri(),
            request.getEntry().getDependentEntryIdsList());
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(LzyWhiteboard.CommitCommand request,
                       StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::commit " + JsonUtils.printRequest(request));
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository
            .resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        final SnapshotEntry entry = repository
            .resolveEntry(snapshotStatus.snapshot(), request.getEntryId());
        if (entry == null) {
            responseObserver.onError(Status.NOT_FOUND.asException());
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
    public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request,
                                 StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::finalizeSnapshot " + JsonUtils.printRequest(request));
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = repository
            .resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null
            || !Objects.equals(snapshotStatus.snapshot().uid().toString(), request.getAuth().getUser().getUserId())) {
            responseObserver.onError(Status.NOT_FOUND.asException());
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

    @Override
    public void entryStatus(LzyWhiteboard.EntryStatusCommand request,
                            StreamObserver<LzyWhiteboard.EntryStatusResponse> responseObserver) {
        LOG.info("SnapshotApi::entryStatus " + JsonUtils.printRequest(request));
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        SnapshotStatus snapshotStatus = repository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            LOG.info("SnapshotApi::entryStatus snapshot {} not found", request.getSnapshotId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Snapshot " + request.getSnapshotId() + " not found").asException());
            return;
        }
        SnapshotEntryStatus entry = repository.resolveEntryStatus(snapshotStatus.snapshot(), request.getEntryId());
        if (entry == null) {
            LOG.info("SnapshotApi::entryStatus entry {} not found, creating", request.getEntryId());
            entry = repository.createEntry(snapshotStatus.snapshot(), request.getEntryId());
        }
        LzyWhiteboard.EntryStatusResponse.Builder builder = LzyWhiteboard.EntryStatusResponse.newBuilder()
            .setSnapshotId(snapshotStatus.snapshot().id().toString())
            .setEntryId(entry.entry().id())
            .setStatus(LzyWhiteboard.EntryStatusResponse.Status.valueOf(entry.status().name()))
            .setEmpty(entry.empty());
        URI storage = entry.storage();
        if (storage != null) {
            builder.setStorageUri(storage.toString());
        }
        LzyWhiteboard.EntryStatusResponse resp = builder.build();
        LOG.info("SnapshotApi::entryStatus status: " + JsonUtils.printRequest(resp));
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }
}
