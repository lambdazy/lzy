package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;
import java.util.UUID;

public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {
    private final SnapshotRepository repository;

    public SnapshotApi(SnapshotRepository repository) {
        this.repository = repository;
    }

    @Override
    public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request, StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
        //TODO: auth
        URI snapshotId = URI.create(UUID.randomUUID().toString());
        repository.create(new Snapshot.Impl(snapshotId));
        final LzyWhiteboard.Snapshot result = LzyWhiteboard.Snapshot
                .newBuilder()
                .setSnapshotId(snapshotId.toString())
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void prepareToSave(LzyWhiteboard.PrepareCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        //TODO: auth
        final SnapshotStatus snapshotStatus = repository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        repository.prepare(gRPCConverter.from(request.getEntry(), snapshotStatus.snapshot()));
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(LzyWhiteboard.CommitCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        //TODO: auth
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
        repository.commit(entry);
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        //TODO: auth
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
