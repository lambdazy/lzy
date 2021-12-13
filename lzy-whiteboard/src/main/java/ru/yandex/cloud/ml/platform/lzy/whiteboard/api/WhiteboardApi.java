package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
@Requires(property = "server.uri")
public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {
    private final WhiteboardRepository whiteboardRepository;
    private final SnapshotRepository snapshotRepository;
    private final Authenticator auth;

    @Inject
    public WhiteboardApi(ServerConfig serverConfig, WhiteboardRepository whiteboardRepository, SnapshotRepository snapshotRepository) {
        URI uri = URI.create(serverConfig.getUri());
        final ManagedChannel serverChannel = ManagedChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
        auth = new SimpleAuthenticator(LzyServerGrpc.newBlockingStub(serverChannel));
        this.whiteboardRepository = whiteboardRepository;
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request, StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
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
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
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
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final WhiteboardStatus whiteboardStatus = whiteboardRepository.resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(whiteboardStatus.whiteboard())
                .filter(field -> field.entry() != null)
                .map(field -> {
                        SnapshotEntryStatus entryStatus = whiteboardRepository.resolveEntryStatus(
                            field.entry().snapshot(), field.entry().id()
                        );
                        if (entryStatus == null) {
                            return null;
                        }
                        return gRPCConverter.to(
                            field,
                            whiteboardRepository.dependent(field).collect(Collectors.toList()),
                            entryStatus.empty(),
                            entryStatus.storage().toString()
                        );
                    }
                )
                .collect(Collectors.toList());
        final LzyWhiteboard.Whiteboard result = LzyWhiteboard.Whiteboard
                .newBuilder()
                .setSnapshot(gRPCConverter.to(whiteboardStatus.whiteboard().snapshot()))
                .addAllFields(fields)
                .setStatus(gRPCConverter.to(whiteboardStatus.state()))
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}
