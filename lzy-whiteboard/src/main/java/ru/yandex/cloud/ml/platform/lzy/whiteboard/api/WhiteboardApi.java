package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {
    private final WhiteboardRepository whiteboardRepository;
    private final SnapshotRepository snapshotRepository;

    public WhiteboardApi(WhiteboardRepository whiteboardRepository, SnapshotRepository snapshotRepository) {
        this.whiteboardRepository = whiteboardRepository;
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request, StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        //TODO: auth
        final SnapshotStatus snapshotStatus = snapshotRepository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        URI wbId = URI.create(UUID.randomUUID().toString());
        whiteboardRepository.create(new Whiteboard.Impl(wbId, new HashSet<>(request.getFieldNamesList()), snapshotStatus.snapshot()));
        final LzyWhiteboard.Whiteboard result = buildWhiteboard(wbId, responseObserver);
        if (result != null)
            responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void link(LzyWhiteboard.LinkCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        //TODO: auth
        final WhiteboardStatus whiteboardStatus = whiteboardRepository.resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Cannot found whiteboard " + request.getWhiteboardId()).asException());
            return;
        }
        final SnapshotEntry snapshotEntry = snapshotRepository.resolveEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId());
        if (snapshotEntry == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Cannot found snapshot entry " + request.getEntryId()).asException());
            return;
        }
        whiteboardRepository.add(new WhiteboardField.Impl(request.getFieldName(), snapshotEntry, whiteboardStatus.whiteboard()));
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request, StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        //TODO: auth
        final LzyWhiteboard.Whiteboard result = buildWhiteboard(URI.create(request.getWhiteboardId()), responseObserver);
        if (result != null)
            responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    private LzyWhiteboard.Whiteboard buildWhiteboard(URI id, StreamObserver<LzyWhiteboard.Whiteboard> responseObserver){
        WhiteboardStatus wb = whiteboardRepository.resolveWhiteboard(id);
        if (wb == null){
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot found whiteboard with id " + id).asException());
            return null;
        }
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(wb.whiteboard())
                .map(field -> gRPCConverter.to(field, whiteboardRepository.dependent(field).collect(Collectors.toList())))
                .collect(Collectors.toList());
        return LzyWhiteboard.Whiteboard.newBuilder()
                .setId(wb.whiteboard().id().toString())
                .setStatus(gRPCConverter.to(wb.state()))
                .setSnapshot(LzyWhiteboard.Snapshot.newBuilder()
                        .setSnapshotId(wb.whiteboard().snapshot().id().toString())
                        .build())
                .addAllFields(fields)
                .setStatus(gRPCConverter.to(whiteboardStatus.state()))
                .build();
    }
}
