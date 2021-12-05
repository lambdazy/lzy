package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {
    private final WhiteboardRepository whiteboardRepository;
    private final SnapshotRepository snapshotRepository;

    public WhiteboardApi(WhiteboardRepository whiteboardRepository, SnapshotRepository snapshotRepository) {
        this.whiteboardRepository = whiteboardRepository;
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request, StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        final SnapshotStatus snapshotStatus = snapshotRepository.resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        URI wbId = URI.create(UUID.randomUUID().toString());
        whiteboardRepository.create(new Whiteboard.Impl(wbId, new HashSet<>(request.getFieldNamesList()), snapshotStatus.snapshot()));
        final LzyWhiteboard.Whiteboard id = LzyWhiteboard.Whiteboard
                .newBuilder()
                .setId(wbId.toString())
                .build();
        responseObserver.onNext(id);
        responseObserver.onCompleted();
    }

    @Override
    public void link(LzyWhiteboard.LinkCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        final WhiteboardStatus whiteboardStatus = whiteboardRepository.resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        final SnapshotEntry snapshotEntry = snapshotRepository.resolveEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId());
        if (snapshotEntry == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
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
        final WhiteboardStatus whiteboardStatus = whiteboardRepository.resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(whiteboardStatus.whiteboard())
                .map(field -> gRPCConverter.to(field, whiteboardRepository.dependent(field).collect(Collectors.toList())))
                .collect(Collectors.toList());
        final LzyWhiteboard.Whiteboard result = LzyWhiteboard.Whiteboard
                .newBuilder()
                .setSnapshot(gRPCConverter.to(whiteboardStatus.whiteboard().snapshot()))
                .addAllFields(fields)
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}
