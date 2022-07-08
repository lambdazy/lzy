package ai.lzy.whiteboard.api;

import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.whiteboard.SnapshotRepository;
import ai.lzy.whiteboard.WhiteboardRepository;
import ai.lzy.whiteboard.auth.Authenticator;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.snapshot.SnapshotEntry;
import ai.lzy.model.snapshot.SnapshotEntryStatus;
import ai.lzy.model.snapshot.SnapshotStatus;
import ai.lzy.model.snapshot.Whiteboard;
import ai.lzy.model.snapshot.Whiteboard.Impl;
import ai.lzy.model.snapshot.WhiteboardField;
import ai.lzy.model.snapshot.WhiteboardStatus;
import ai.lzy.model.snapshot.WhiteboardStatus.State;
import ai.lzy.model.utils.Permissions;
import ai.lzy.whiteboard.auth.SimpleAuthenticator;
import ai.lzy.whiteboard.config.ServiceConfig;
import ai.lzy.whiteboard.exceptions.WhiteboardRepositoryException;
import ai.lzy.priv.v2.LzyServerGrpc;
import ai.lzy.priv.v2.LzyWhiteboard;
import ai.lzy.priv.v2.WbApiGrpc;

@Singleton
@Requires(property = "service.server-uri")
public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardApi.class);
    private final WhiteboardRepository whiteboardRepository;
    private final SnapshotRepository snapshotRepository;
    private final Authenticator auth;

    @Inject
    public WhiteboardApi(ServiceConfig serviceConfig, WhiteboardRepository whiteboardRepository,
                         SnapshotRepository snapshotRepository) {
        URI uri = URI.create(serviceConfig.getServerUri());
        final ManagedChannel serverChannel = ChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        auth = new SimpleAuthenticator(LzyServerGrpc.newBlockingStub(serverChannel));
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
        LOG.info("WhiteboardApi::createWhiteboard: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("WhiteboardApi::createWhiteboard: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied to create whiteboard").asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = snapshotRepository
            .resolveSnapshot(URI.create(request.getSnapshotId()));
        if (snapshotStatus.isEmpty()
            || !Objects.equals(snapshotStatus.get().snapshot().uid().toString(),
            request.getAuth().getUser().getUserId())) {
            LOG.error("WhiteboardApi::createWhiteboard: Could not find snapshot with id {} ", request.getSnapshotId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        LOG.info("WhiteboardApi::createWhiteboard: Resolved snapshot: {} ", snapshotStatus);
        URI wbId = URI.create(UUID.randomUUID().toString());
        try {
            final WhiteboardStatus status = whiteboardRepository.create(
                new Impl(wbId, new HashSet<>(request.getFieldNamesList()),
                    snapshotStatus.get().snapshot(), new HashSet<>(request.getTagsList()),
                    resolveNamespace(request.getNamespace(), request.getAuth().getUser().getUserId()),
                    GrpcConverter.from(request.getCreationDateUTC())));
            responseObserver.onNext(buildWhiteboard(status));
            responseObserver.onCompleted();
        } catch (WhiteboardRepositoryException e) {
            LOG.error("WhiteboardApi::createWhiteboard: Got exception while creating whiteboard {}", e.getMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void link(LzyWhiteboard.LinkCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("WhiteboardApi::link: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("WhiteboardApi::link: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for link command").asException());
            return;
        }
        final Optional<WhiteboardStatus> whiteboardStatus = whiteboardRepository
            .resolveWhiteboard(URI.create(request.getWhiteboardId()));

        if (whiteboardStatus.isEmpty()
            || !Objects.equals(whiteboardStatus.get().whiteboard().snapshot().uid().toString(),
            request.getAuth().getUser().getUserId())) {
            responseObserver.onError(
                Status.NOT_FOUND
                    .withDescription("Could not find whiteboard with id " + request.getWhiteboardId())
                    .asException()
            );
            return;
        }

        final Whiteboard wb = whiteboardStatus.get().whiteboard();
        SnapshotEntry snapshotEntry = snapshotRepository
            .resolveEntry(wb.snapshot(), request.getEntryId())
            .orElseGet(() -> {
                LOG.info("WhiteboardApi::link: Could not resolve snapshot entry with id {} ", request.getEntryId());
                return new SnapshotEntry.Impl(request.getEntryId(), wb.snapshot());
            });

        try {
            whiteboardRepository.update(new WhiteboardField.Impl(request.getFieldName(), snapshotEntry, wb));
        } catch (WhiteboardRepositoryException e) {
            LOG.error("WhiteboardApi::link: Got exception while linking {}", e.getMessage());
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        }

        LOG.info("WhiteboardApi::link: Successfully linked whiteboard field");
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
        LOG.info("WhiteboardApi::getWhiteboard: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("WhiteboardApi::getWhiteboard: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for getWhiteboard command").asException());
            return;
        }
        final Optional<WhiteboardStatus> resolved = whiteboardRepository
            .resolveWhiteboard(URI.create(request.getWhiteboardId()));
        if (resolved.isEmpty()
            || !Objects.equals(resolved.get().whiteboard().snapshot().uid().toString(),
            request.getAuth().getUser().getUserId())) {
            LOG.error("WhiteboardApi::getWhiteboard: Could not resolve whiteboard with id {} ",
                request.getWhiteboardId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not resolve whiteboard with id " + request.getWhiteboardId())
                    .asException());
            return;
        }
        final WhiteboardStatus whiteboardStatus = resolved.get();
        if (whiteboardStatus.state().equals(State.ERRORED)) {
            LOG.error("WhiteboardApi::getWhiteboard: Whiteboard {} is in errored condition", request.getWhiteboardId());
            responseObserver.onError(
                Status.UNKNOWN.withDescription("Whiteboard is in errored condition").asException());
            return;
        }
        if (!whiteboardStatus.state().equals(State.COMPLETED)) {
            LOG.error("WhiteboardApi::getWhiteboard: Whiteboard {} is not completed", request.getWhiteboardId());
            responseObserver.onError(
                Status.FAILED_PRECONDITION.withDescription("Whiteboard is not completed").asException());
            return;
        }
        try {
            final LzyWhiteboard.Whiteboard result = buildWhiteboard(whiteboardStatus);
            LOG.info("WhiteboardApi::getWhiteboard: Successfully executed get whiteboard command for whiteboard {}",
                request.getWhiteboardId());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (WhiteboardRepositoryException e) {
            LOG.error("WhiteboardApi::getWhiteboard: Got exception while getting whiteboard with id {}: {}",
                request.getWhiteboardId(), e.getMessage());
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException()
            );
        }
    }

    private LzyWhiteboard.Whiteboard buildWhiteboard(WhiteboardStatus wb) throws WhiteboardRepositoryException {
        List<LzyWhiteboard.WhiteboardField> fields = whiteboardRepository.fields(wb.whiteboard())
            .map(field -> {
                    final List<WhiteboardField> dependent = whiteboardRepository.dependent(field)
                        .collect(Collectors.toList());
                    final SnapshotEntry entry = field.entry();
                    if (entry == null) {
                        return GrpcConverter.to(field, dependent, null);
                    }
                    Optional<SnapshotEntryStatus> entryStatus = snapshotRepository.resolveEntryStatus(
                        entry.snapshot(), entry.id()
                    );
                    if (entryStatus.isEmpty()) {
                        throw new IllegalArgumentException("Cannot find snapshot entry: " + entry.id());
                    }
                    return GrpcConverter.to(
                        field,
                        dependent,
                        entryStatus.get()
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
        LOG.info("WhiteboardApi::whiteboardsList: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("WhiteboardApi::whiteboardsList: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for whiteboardsList command")
                    .asException());
            return;
        }
        Date fromDate = Date.from(Instant.ofEpochSecond(LocalDateTime.of(1, 1, 1, 0, 0, 0, 0)
            .toEpochSecond(ZoneOffset.UTC)));
        Date toDate = Date.from(Instant.ofEpochSecond(LocalDateTime.of(9999, 12, 31, 23, 59, 59)
            .toEpochSecond(ZoneOffset.UTC)));
        if (request.hasFromDateUTC()) {
            fromDate = GrpcConverter.from(request.getFromDateUTC());
            LOG.info("WhiteboardApi::whiteboardsList: Parsed date lower bound to {}",
                new SimpleDateFormat("dd MMMM yyyy zzzz").format(fromDate));
        }
        if (request.hasToDateUTC()) {
            toDate = GrpcConverter.from(request.getToDateUTC());
            LOG.info("WhiteboardApi::whiteboardsList: Parsed date upper bound to {}",
                new SimpleDateFormat("dd MMMM yyyy zzzz").format(toDate));
        }
        final List<WhiteboardStatus> whiteboardStatus = whiteboardRepository.resolveWhiteboards(
            resolveNamespace(request.getNamespace(), request.getAuth().getUser().getUserId()),
            request.getTagsList(), fromDate, toDate
        ).collect(Collectors.toList());
        try {
            List<LzyWhiteboard.Whiteboard> result = new ArrayList<>();
            for (var entry : whiteboardStatus) {
                result.add(buildWhiteboard(entry));
            }
            LOG.info("WhiteboardApi::whiteboardsList: Successfully resolved whiteboard list");
            final LzyWhiteboard.WhiteboardsResponse response = LzyWhiteboard.WhiteboardsResponse.newBuilder()
                .addAllWhiteboards(result)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (WhiteboardRepositoryException e) {
            LOG.error("WhiteboardApi::whiteboardsList: Got exception while getting list of whiteboards {}",
                e.getMessage());
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException()
            );
        }
    }
}
