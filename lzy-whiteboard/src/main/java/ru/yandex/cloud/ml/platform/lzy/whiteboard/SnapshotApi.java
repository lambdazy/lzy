package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.stub.StreamObserver;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {
    // IN_PROGRESS --> started saving data
    // FINISHED --> finished saving data
    enum SlotStatus {
        IN_PROGRESS,
        FINISHED
    }

    enum SnapshotStatus {
        CREATED,
        FINALIZED,
        ERRORED
    }

    private final ConcurrentHashMap<URI, Set<StorageBinding>> storageBindings = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, SnapshotApi.SnapshotStatus> snapshotStatus = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, String> owners = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, Set<String>> deps = new ConcurrentHashMap<>();

    private static class StorageBinding {
        private final String snapshotId;
        private final String entryId;
        private final URI storageUri;
        private SnapshotApi.SlotStatus status;
        private boolean isEmpty = true;

        private StorageBinding(String snapshotId, String entryId, String storageUri) {
            this.snapshotId = snapshotId;
            this.entryId = entryId;
            this.storageUri = URI.create(storageUri);
            status = SnapshotApi.SlotStatus.IN_PROGRESS;
        }

        public void setStatus(SnapshotApi.SlotStatus newStatus) {
            status = newStatus;
        }

        public String getSnapshotId() {
            return snapshotId;
        }

        public static LzyWhiteboard.StorageBinding to(StorageBinding bindings) {
            return LzyWhiteboard.StorageBinding
                    .newBuilder()
                    .setFieldName(bindings.entryId)
                    .setStorageUri(bindings.storageUri.toString())
                    .build();
        }

        public void setEmpty(boolean empty) {
            isEmpty = empty;
        }
    }

    @Override
    public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request, StreamObserver<LzyWhiteboard.SnapshotId> responseObserver) {
        URI snapshotId = URI.create(UUID.randomUUID().toString());
        final LzyWhiteboard.SnapshotId result = LzyWhiteboard.SnapshotId
                .newBuilder()
                .setSnapshotId(snapshotId.toString())
                .build();
        snapshotStatus.put(snapshotId, SnapshotStatus.CREATED);
        owners.put(snapshotId, request.getUserCredentials().getUserId());
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void prepareToSave(LzyWhiteboard.PrepareCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        storageBindings.putIfAbsent(URI.create(request.getSnapshotId()), new HashSet<>());
        storageBindings.computeIfPresent(URI.create(request.getSnapshotId()),
                (k, v) -> {
                    v.add(new StorageBinding(request.getSnapshotId(), request.getEntryId(), request.getUri()));
                    return v;
                });
        if (request.hasDependency()) {
            LzyWhiteboard.Dependency dependency = request.getDependency();
            deps.putIfAbsent(URI.create(request.getEntryId()), new HashSet<>());
            deps.computeIfPresent(URI.create(request.getEntryId()),
                    (k, v) -> {
                        List<String> list = dependency.getDepEntryIdList();
                        v.addAll(list);
                        return v;
                    });
        }
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(LzyWhiteboard.CommitCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        storageBindings.computeIfPresent(URI.create(request.getSnapshotId()),
                (k, v) -> {
                    for (var sb : v) {
                        if (sb.entryId.equals(request.getEntryId())) {
                            sb.setStatus(SlotStatus.FINISHED);
                            sb.setEmpty(request.getEmpty());
                        }
                    }
                    return v;
                });
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        snapshotStatus.replace(URI.create(request.getSnapshotId()), SnapshotStatus.FINALIZED);
        // TODO: go through all whiteboards and set status finalized
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllLinks(LzyWhiteboard.GetAllLinksCommand request, StreamObserver<LzyWhiteboard.GetAllLinksResponse> responseObserver) {
        final LzyWhiteboard.GetAllLinksResponse.Builder builder = LzyWhiteboard.GetAllLinksResponse.newBuilder();
        var bindings = storageBindings.get(URI.create(request.getSnapshotId()));
        for (var binding : bindings) {
            builder.addStorageBindings(StorageBinding.to(binding));
        }
        for (var storageBinding : bindings) {
            if (deps.get(URI.create(storageBinding.entryId)) != null) {
                builder.addRelation(LzyWhiteboard.Relation
                        .newBuilder()
                        .setFieldName(storageBinding.entryId)
                        .addAllDependencies(deps.get(URI.create(storageBinding.entryId)))
                        .build());
            }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
