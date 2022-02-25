package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import static ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter.to;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard.Impl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.LzyBackofficeGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

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

    private String resolveNamespace(String namespace, String uid) {
        if (!namespace.startsWith(uid + ":")) {
            return uid + ":" + namespace;
        }
        return namespace;
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
        if (snapshotStatus == null
            || !Objects.equals(snapshotStatus.snapshot().uid().toString(), request.getAuth().getUser().getUserId())) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        URI wbId = URI.create(UUID.randomUUID().toString());
        try {
            final WhiteboardStatus status = whiteboardRepository.create(
                new Impl(wbId, new HashSet<>(request.getFieldNamesList()),
                    snapshotStatus.snapshot(), new HashSet<>(request.getTagsList()),
                    resolveNamespace(request.getNamespace(), request.getAuth().getUser().getUserId()),
                    GrpcConverter.from(request.getCreationDateUTC())));
            responseObserver.onNext(buildWhiteboard(status));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
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
        if (whiteboardStatus == null
            || !Objects.equals(whiteboardStatus.whiteboard().snapshot().uid().toString(),
            request.getAuth().getUser().getUserId())) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Cannot find whiteboard " + request.getWhiteboardId())
                .asException());
            return;
        }
        SnapshotEntry snapshotEntry = snapshotRepository
            .resolveEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId());
        if (snapshotEntry == null) {
            snapshotEntry = snapshotRepository
                .createEntry(whiteboardStatus.whiteboard().snapshot(), request.getEntryId())
                .entry();
        }
        whiteboardRepository.update(new WhiteboardField.Impl(request.getFieldName(), snapshotEntry,
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
        if (whiteboardStatus == null
            || !Objects.equals(whiteboardStatus.whiteboard().snapshot().uid().toString(),
            request.getAuth().getUser().getUserId())) {
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }
        final LzyWhiteboard.Whiteboard result = buildWhiteboard(whiteboardStatus);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    private LzyWhiteboard.Whiteboard buildWhiteboard(WhiteboardStatus wb) {
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(wb.whiteboard())
            .map(field -> {
                    final List<WhiteboardField> dependent = whiteboardRepository.dependent(field)
                        .collect(Collectors.toList());
                    final SnapshotEntry entry = field.entry();
                    if (entry == null) {
                        return GrpcConverter.to(field, dependent, null);
                    }
                    SnapshotEntryStatus entryStatus = snapshotRepository.resolveEntryStatus(
                        entry.snapshot(), entry.id()
                    );
                    if (entryStatus == null) {
                        throw new RuntimeException("Cannot find snapshot entry: " + entry.id());
                    }
                    return GrpcConverter.to(
                        field,
                        dependent,
                        entryStatus
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
    public void whiteboardsList(
        LzyWhiteboard.WhiteboardsListCommand request,
        StreamObserver<LzyWhiteboard.WhiteboardsResponse> responseObserver) {
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            responseObserver.onError(Status.PERMISSION_DENIED.asException());
            return;
        }
        Date fromDate = Date.from(Instant.ofEpochSecond(LocalDateTime.of(1, 1, 1, 0, 0, 0, 0)
            .toEpochSecond(ZoneOffset.UTC)));
        Date toDate = Date.from(Instant.ofEpochSecond(LocalDateTime.of(9999, 12, 31, 23, 59, 59)
            .toEpochSecond(ZoneOffset.UTC)));
        if (request.hasFromDateUTC()) {
            fromDate = GrpcConverter.from(request.getFromDateUTC());
        }
        if (request.hasToDateUTC()) {
            toDate = GrpcConverter.from(request.getToDateUTC());
        }
        final List<WhiteboardStatus> whiteboardStatus = whiteboardRepository.resolveWhiteboards(
            resolveNamespace(request.getNamespace(), request.getAuth().getUser().getUserId()),
            request.getTagsList(), fromDate, toDate
        ).collect(Collectors.toList());
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
