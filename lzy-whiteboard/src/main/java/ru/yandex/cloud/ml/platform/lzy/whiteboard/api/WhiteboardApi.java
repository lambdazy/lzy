package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.LzySnapshot;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

import static ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter.to;

@Singleton
@Requires(property = "server.uri")
public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {

    private final WhiteboardRepository whiteboardRepository;
    private final SnapshotRepository snapshotRepository;
    private final Authenticator auth;

    @Inject
    public WhiteboardApi(ServerConfig serverConfig, WhiteboardRepository whiteboardRepository,
        SnapshotRepository snapshotRepository) {
        URI uri = URI.create(serverConfig.getUri());
        final ManagedChannel serverChannel = ChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        auth = new SimpleAuthenticator(LzyServerGrpc.newBlockingStub(serverChannel),
                LzyBackofficeGrpc.newBlockingStub(serverChannel));
        this.whiteboardRepository = whiteboardRepository;
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request,
        StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final SnapshotStatus snapshotStatus = snapshotRepository
            .resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        URI wbId = URI.create(UUID.randomUUID().toString());
        whiteboardRepository.create(
            new Whiteboard.Impl(wbId, new HashSet<>(request.getFieldNamesList()),
                snapshotStatus.snapshot(), new HashSet<>(request.getTagsList()), request.getNamespace()));
        final LzyWhiteboard.Whiteboard result = buildWhiteboard(whiteboardRepository.resolveWhiteboard(wbId));
        if (result != null) {
            responseObserver.onNext(result);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void link(LzyWhiteboard.LinkCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final WhiteboardStatus whiteboardStatus = whiteboardRepository
            .resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Cannot find whiteboard " + request.getWhiteboardId())
                .asException());
            return;
        }
        SnapshotEntry snapshotEntry = snapshotRepository
            .resolveEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId());
        if (snapshotEntry == null) {
            snapshotEntry = snapshotRepository
                .createEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId());
        }
        whiteboardRepository.add(new WhiteboardField.Impl(request.getFieldName(), snapshotEntry,
            whiteboardStatus.whiteboard()));
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request,
        StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final WhiteboardStatus whiteboardStatus = whiteboardRepository
            .resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (whiteboardStatus == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        final LzyWhiteboard.Whiteboard result = buildWhiteboard(whiteboardStatus);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    private LzyWhiteboard.Whiteboard buildWhiteboard(WhiteboardStatus wb) {
        if (wb == null) {
            return null;
        }
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(wb.whiteboard())
            .filter(field -> field.entry() != null)
            .map(field -> {
                    SnapshotEntryStatus entryStatus = snapshotRepository.resolveEntryStatus(
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
        return LzyWhiteboard.Whiteboard.newBuilder()
            .setId(wb.whiteboard().id().toString())
            .setStatus(to(wb.state()))
            .setSnapshot(LzyWhiteboard.Snapshot.newBuilder()
                .setSnapshotId(wb.whiteboard().snapshot().id().toString())
                .build())
            .addAllFields(fields)
            .addAllTags(wb.whiteboard().tags())
            .setNamespace(wb.whiteboard().namespace())
            .setStatus(to(wb.state()))
            .build();
    }

    @Override
    public void whiteboards(LzyWhiteboard.WhiteboardsCommand request, StreamObserver<LzyWhiteboard.WhiteboardsInfo> responseObserver) {
        boolean ok = true;
        String uid = "";
        if (request.hasAuth()) {
            ok = auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL);
            uid = request.getAuth().getUser().getUserId();
        }
        if (request.hasBackoffice()) {
            ok = auth.checkPermissions(request.getBackoffice(), Permissions.WHITEBOARD_ALL);
            uid = request.getBackoffice().getBackofficeCredentials().getUserId();
        }
        if (!ok) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }

        List<WhiteboardInfo> wbInfoList = whiteboardRepository.whiteboards(URI.create(uid));
        final LzyWhiteboard.WhiteboardsInfo response = LzyWhiteboard.WhiteboardsInfo
                .newBuilder()
                .addAllWhiteboards(wbInfoList.stream().map(gRPCConverter::to).collect(Collectors.toList()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void whiteboardByNamespaceAndTags(LzyWhiteboard.WhiteboardByNamespaceAndTagsCommand request,
                                             StreamObserver<LzyWhiteboard.WhiteboardsResponse> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        final List<WhiteboardStatus> whiteboardStatus = whiteboardRepository
                .resolveWhiteboards(request.getNamespace(), request.getTagsList());
        List<LzyWhiteboard.Whiteboard> result = new ArrayList<>();
        for (var entry : whiteboardStatus) {
            result.add(buildWhiteboard(entry));
        }
        final LzyWhiteboard.WhiteboardsResponse response = LzyWhiteboard.WhiteboardsResponse.newBuilder()
            .addAllWhiteboards(result)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
