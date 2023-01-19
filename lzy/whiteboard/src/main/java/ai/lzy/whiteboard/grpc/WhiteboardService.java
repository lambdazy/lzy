package ai.lzy.whiteboard.grpc;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LWBS.*;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.access.AccessManager;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.whiteboard.grpc.ProtoValidator.isValid;

public class WhiteboardService extends LzyWhiteboardServiceGrpc.LzyWhiteboardServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final AccessManager accessManager;
    private final WhiteboardStorage whiteboardStorage;
    private final WhiteboardDataSource dataSource;

    private final OperationDao operationDao;

    @Inject
    public WhiteboardService(AccessManager accessManager,
                             WhiteboardStorage whiteboardStorage,
                             WhiteboardDataSource dataSource,
                             @Named("WhiteboardOperationDao") OperationDao operationDao)
    {
        this.accessManager = accessManager;
        this.whiteboardStorage = whiteboardStorage;
        this.dataSource = dataSource;
        this.operationDao = operationDao;
    }

    @Override
    public void get(LWBS.GetRequest request, StreamObserver<LWBS.GetResponse> responseObserver) {
        LOG.info("Get whiteboard {}", request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String whiteboardId = request.getWhiteboardId();

            if (whiteboardId.isBlank()) {
                final String errorMessage = "ID is empty";
                LOG.error("Get whiteboard {} failed, invalid argument: {}", request.getWhiteboardId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            }

            if (!accessManager.checkAccess(userId, whiteboardId)) {
                LOG.error("Get whiteboard {} failed, permission denied.", request.getWhiteboardId());
                final String clientErrorMessage = "Whiteboard " + whiteboardId + " not found";
                responseObserver.onError(Status.NOT_FOUND.withDescription(clientErrorMessage).asException());
            }

            final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, null);
            responseObserver.onNext(LWBS.GetResponse.newBuilder()
                .setWhiteboard(ProtoConverter.toProto(whiteboard))
                .build());
            LOG.info("Get whiteboard {} done", whiteboardId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Get whiteboard {} failed, invalid argument: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (NotFoundException e) {
            LOG.error("Get whiteboard {} failed, not found exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Get whiteboard {} failed, got exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void list(LWBS.ListRequest request, StreamObserver<LWBS.ListResponse> responseObserver) {
        LOG.info("List whiteboards");

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

            @Nullable String name = request.getName().isBlank() ? null : request.getName();
            List<String> tags = request.getTagsList();
            @Nullable Instant createdAtLowerBound = null;

            if (request.hasCreatedTimeBounds() && request.getCreatedTimeBounds().hasFrom()) {
                createdAtLowerBound = ai.lzy.util.grpc.ProtoConverter.fromProto(
                    request.getCreatedTimeBounds().getFrom());
            }

            @Nullable Instant createdAtUpperBound = null;
            if (request.hasCreatedTimeBounds() && request.getCreatedTimeBounds().hasTo()) {
                createdAtUpperBound = ai.lzy.util.grpc.ProtoConverter.fromProto(request.getCreatedTimeBounds().getTo());
            }

            Stream<Whiteboard> whiteboards = whiteboardStorage.listWhiteboards(userId, name, tags,
                createdAtLowerBound, createdAtUpperBound, null);

            var response = LWBS.ListResponse.newBuilder()
                .addAllWhiteboards(whiteboards.map(ProtoConverter::toProto).toList())
                .build();
            responseObserver.onNext(response);
            LOG.info("List whiteboards done, {} found", response.getWhiteboardsCount());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("List whiteboards failed, invalid argument: {}", e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("List whiteboards failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void registerWhiteboard(RegisterWhiteboardRequest request,
                                   StreamObserver<RegisterWhiteboardResponse> responseObserver)
    {
        if (!validateRegisterRequest(request, responseObserver)) {
            return;
        }

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, RegisterWhiteboardResponse.class,
                Duration.ofMillis(100), Duration.ofSeconds(1), LOG))
        {
            return;
        }

        var authenticationContext = AuthenticationContext.current();
        var userId = Objects.requireNonNull(authenticationContext).getSubject().id();

        final LWB.Whiteboard whiteboard = request.getWhiteboard();
        var op = Operation.create(
            userId,
            "Register whiteboard: namespace=%s, name=%s, id=%s".formatted(whiteboard.getNamespace(),
                whiteboard.getName(), whiteboard.getId()),
            /* deadline */ null,
            idempotencyKey,
            /* meta */ null);

        try {
            withRetries(LOG, () -> operationDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao,
                responseObserver, RegisterWhiteboardResponse.class, Duration.ofMillis(100), Duration.ofSeconds(1), LOG))
            {
                return;
            }

            LOG.error("Cannot create operation of whiteboard creation: { namespace: {}, whiteboardName: {} }, " +
                "error: {}", whiteboard.getNamespace(), whiteboard.getName(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        LOG.info("Register whiteboard name={}, id={}", whiteboard.getName(), whiteboard.getId());
        try {
            final Instant inserted = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            withRetries(defaultRetryPolicy(), LOG, () -> whiteboardStorage.registerWhiteboard(
                userId, ProtoConverter.fromProto(whiteboard), inserted, null)
            );

            try {
                accessManager.addAccess(userId, whiteboard.getId());
            } catch (Exception e) {
                String errorMessage = "Failed to get access to whiteboard for user " + userId;
                LOG.error("Create whiteboard {} failed, got exception: {}", whiteboard.getName(),
                    errorMessage, e);
                LOG.info("Undo creating whiteboard {}, id = {}", whiteboard.getName(), whiteboard.getId());
                withRetries(defaultRetryPolicy(), LOG, () ->
                    whiteboardStorage.deleteWhiteboard(whiteboard.getId(), null));
                LOG.info("Undo creating whiteboard {} done", whiteboard.getName());
                var status = Status.INTERNAL.withCause(e);

                operationDao.failOperation(op.id(), toProto(status), null, LOG);

                responseObserver.onError(status.asRuntimeException());
            }
        } catch (IllegalArgumentException e) {
            LOG.error("Create whiteboard {} failed, invalid argument: {}", whiteboard.getName(),
                e.getMessage(), e);
            var status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());

            try {
                operationDao.failOperation(op.id(), toProto(status), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        } catch (Exception e) {
            LOG.error("Create whiteboard {} failed, got exception: {}", whiteboard.getName(), e.getMessage(), e);
            var status = Status.INTERNAL.withCause(e);

            try {
                operationDao.failOperation(op.id(), toProto(status), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }

        LOG.info("Create whiteboard {} done, id = {}", whiteboard.getName(), whiteboard.getId());
        var response = RegisterWhiteboardResponse.newBuilder()
            .build();
        var packedResponse = Any.pack(response);
        try {
            withRetries(LOG, () -> operationDao.complete(op.id(), packedResponse, null));
        } catch (Exception e) {
            LOG.error("Error while executing transaction: {}", e.getMessage(), e);
            var errorStatus = Status.INTERNAL.withDescription("Error while creating whiteboard: " + e.getMessage());
            try {
                operationDao.failOperation(op.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }
            responseObserver.onError(errorStatus.asRuntimeException());
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateWhiteboard(UpdateWhiteboardRequest request,
                                 StreamObserver<UpdateWhiteboardResponse> responseObserver)
    {
        final var authenticationContext = AuthenticationContext.current();
        final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

        final LWB.Whiteboard requestWhiteboard = request.getWhiteboard();
        final String whiteboardId = requestWhiteboard.getId();
        if (whiteboardId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("whiteboard ID is empty").asException());
            return;
        }

        if (!accessManager.checkAccess(userId, whiteboardId)) {
            LOG.error("PERMISSION DENIED: <{}> trying to access whiteboard <{}> without permission",
                userId, whiteboardId);
            responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                "You don't have access to whiteboard <" + whiteboardId + ">").asException());
            return;
        }

        LOG.info("Updating whiteboard {}", whiteboardId);
        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            // should already be completed
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, UpdateWhiteboardResponse.class,
                Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
        {
            return;
        }

        var operationId = UUID.randomUUID().toString();
        var response = UpdateWhiteboardResponse.getDefaultInstance();

        var op = Operation.createCompleted(
            operationId,
            userId,
            "Update whiteboard: whiteboardId=" + whiteboardId,
            idempotencyKey,
            /* meta */ null,
            response);

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var tx = TransactionHandle.create(dataSource)) {
                    operationDao.create(op, tx);
                    // it is not optimal update strategy, but this operation is pretty rare
                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, tx);
                    final Whiteboard.Storage storage = whiteboard.storage();
                    final Whiteboard.Storage newStorage = new Whiteboard.Storage(
                        requestWhiteboard.getStorage().getName().isBlank() ? storage.name() :
                            requestWhiteboard.getStorage().getName(),
                        requestWhiteboard.getStorage().getDescription().isBlank() ? storage.description() :
                            requestWhiteboard.getStorage().getDescription(),
                        requestWhiteboard.getStorage().getUri().isBlank() ? storage.uri() :
                            URI.create(requestWhiteboard.getStorage().getUri())
                    );
                    final Whiteboard newWhiteboard = new Whiteboard(
                        whiteboardId,
                        requestWhiteboard.getName().isBlank() ? whiteboard.name() : requestWhiteboard.getName(),
                        requestWhiteboard.getFieldsList().isEmpty() ? whiteboard.fields() :
                            ProtoConverter.fromProto(requestWhiteboard.getFieldsList()),
                        requestWhiteboard.getTagsList().isEmpty() ? whiteboard.tags() :
                            new HashSet<>(requestWhiteboard.getTagsList()),
                        newStorage,
                        requestWhiteboard.getNamespace().isBlank() ? whiteboard.namespace() :
                            requestWhiteboard.getNamespace(),
                        requestWhiteboard.getStatus() == LWB.Whiteboard.Status.WHITEBOARD_STATUS_UNSPECIFIED ?
                            whiteboard.status() :
                            Whiteboard.Status.valueOf(requestWhiteboard.getStatus().name()),
                        requestWhiteboard.getCreatedAt().getSeconds() == 0 ? whiteboard.createdAt() :
                            ai.lzy.util.grpc.ProtoConverter.fromProto(requestWhiteboard.getCreatedAt())
                    );
                    whiteboardStorage.deleteWhiteboard(whiteboardId, tx);
                    final Instant inserted = Instant.now().truncatedTo(ChronoUnit.MILLIS);
                    whiteboardStorage.registerWhiteboard(userId, newWhiteboard, inserted, tx);
                    tx.commit();
                }
            });

            responseObserver.onNext(UpdateWhiteboardResponse.getDefaultInstance());
            LOG.info("Update whiteboard {} done", whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Update whiteboard {} failed, not found exception: {}",
                whiteboardId, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Update whiteboard {} failed, invalid argument: {}",
                whiteboardId, e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            // should already be completed
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, UpdateWhiteboardResponse.class, Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
            {
                return;
            }

            LOG.error("Cannot create operation of whiteboard updating: { whiteboardId: {} }, " +
                "error: {}", whiteboardId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    private static boolean validateRegisterRequest(RegisterWhiteboardRequest request,
                                                   StreamObserver<RegisterWhiteboardResponse> responseObserver)
    {
        var methodName = "Create whiteboard";
        final LWB.Whiteboard whiteboard = request.getWhiteboard();
        if (whiteboard.getId().isBlank()) {
            return replyValidateError(methodName, "Whiteboard must contain not blank ID",
                responseObserver);
        }
        if (whiteboard.getName().isBlank()) {
            return replyValidateError(methodName, "Whiteboard must contain not blank whiteboard name",
                responseObserver);
        }
        if (whiteboard.getFieldsCount() == 0) {
            return replyValidateError(methodName, "Whiteboard must contain at least one field", responseObserver);
        }
        if (whiteboard.getNamespace().isBlank()) {
            return replyValidateError(methodName, "Whiteboard must contain not blank namespace", responseObserver);
        }
        if (whiteboard.getStorage().getName().isBlank()) {
            return replyValidateError(methodName, "Whiteboard must contain not blank storage name", responseObserver);
        }
        if (whiteboard.getStorage().getUri().isBlank()) {
            return replyValidateError(methodName, "Whiteboard must contain not blank storage uri", responseObserver);
        }
        if (whiteboard.getCreatedAt().getSeconds() == 0) {
            return replyValidateError(methodName, "Whiteboard must contain initialized createdAt", responseObserver);
        }
        if (whiteboard.getStatus() == LWB.Whiteboard.Status.WHITEBOARD_STATUS_UNSPECIFIED) {
            return replyValidateError(methodName, "Whiteboard status is not specified", responseObserver);
        }
        if (whiteboard.getFieldsList().stream().anyMatch(field -> !isValid(field))) {
            return replyValidateError(methodName, "Request contains invalid whiteboard field", responseObserver);
        }
        return true;
    }

    private static boolean replyValidateError(String methodName, String message,
                                              StreamObserver<? extends Message> responseObserver)
    {
        LOG.error("{} failed, invalid argument: {}", methodName, message);
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException());
        return false;
    }
}
