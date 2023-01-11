package ai.lzy.whiteboard.grpc;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LWBS.*;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.access.AccessManager;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.LinkedField;
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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.model.grpc.ProtoConverter.fromProto;
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
                final String errorMessage = "Request shouldn't contain empty fields";
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
    public void createWhiteboard(CreateWhiteboardRequest request,
                                 StreamObserver<CreateWhiteboardResponse> responseObserver)
    {
        if (!validateCreateRequest(request, responseObserver)) {
            return;
        }

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, CreateWhiteboardResponse.class,
                Duration.ofMillis(100), Duration.ofSeconds(1), LOG))
        {
            return;
        }

        var authenticationContext = AuthenticationContext.current();
        var userId = Objects.requireNonNull(authenticationContext).getSubject().id();

        var op = Operation.create(
            userId,
            "Create whiteboard: namespace=%s, name=%s".formatted(request.getNamespace(), request.getWhiteboardName()),
            idempotencyKey,
            /* meta */ null);

        try {
            withRetries(LOG, () -> operationDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao,
                responseObserver, CreateWhiteboardResponse.class, Duration.ofMillis(100), Duration.ofSeconds(1), LOG))
            {
                return;
            }

            LOG.error("Cannot create operation of whiteboard creation: { namespace: {}, whiteboardName: {} }, " +
                "error: {}", request.getNamespace(), request.getWhiteboardName(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        LOG.info("Create whiteboard {}", request.getWhiteboardName());

        var whiteboardId = "whiteboard-" + UUID.randomUUID();
        var createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Map<String, Field> fields = request.getFieldsList().stream()
            .map(ProtoConverter::fromProto)
            .collect(Collectors.toMap(Field::name, x -> x));
        var whiteboard = new Whiteboard(whiteboardId, request.getWhiteboardName(), fields,
            new HashSet<>(request.getTagsList()), ProtoConverter.fromProto(request.getStorage()),
            request.getNamespace(), Whiteboard.Status.CREATED, createdAt);

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> whiteboardStorage.insertWhiteboard(userId, whiteboard, null));

            try {
                accessManager.addAccess(userId, whiteboardId);
            } catch (Exception e) {
                String errorMessage = "Failed to get access to whiteboard for user " + userId;
                LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(),
                    errorMessage, e);
                LOG.info("Undo creating whiteboard {}, id = {}", request.getWhiteboardName(), whiteboardId);
                withRetries(defaultRetryPolicy(), LOG, () ->
                    whiteboardStorage.deleteWhiteboard(whiteboardId, null));
                LOG.info("Undo creating whiteboard {} done", request.getWhiteboardName());
                var status = Status.INTERNAL.withCause(e);

                operationDao.failOperation(op.id(), toProto(status), null, LOG);

                responseObserver.onError(status.asRuntimeException());
            }
        } catch (IllegalArgumentException e) {
            LOG.error("Create whiteboard {} failed, invalid argument: {}", request.getWhiteboardName(),
                e.getMessage(), e);
            var status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());

            try {
                operationDao.failOperation(op.id(), toProto(status), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        } catch (Exception e) {
            LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(), e.getMessage(), e);
            var status = Status.INTERNAL.withCause(e);

            try {
                operationDao.failOperation(op.id(), toProto(status), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }

        LOG.info("Create whiteboard {} done, id = {}", request.getWhiteboardName(), whiteboardId);

        var response = CreateWhiteboardResponse.newBuilder()
            .setWhiteboard(ProtoConverter.toProto(whiteboard))
            .build();
        var packedResponse = Any.pack(response);

        try {
            withRetries(LOG, () -> operationDao.complete(op.id(), packedResponse.toByteArray(), null));
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
    public void linkField(LinkFieldRequest request, StreamObserver<LinkFieldResponse> responseObserver) {
        if (!validateLinkRequest(request, responseObserver)) {
            return;
        }

        var authenticationContext = AuthenticationContext.current();
        var userId = Objects.requireNonNull(authenticationContext).getSubject().id();

        var whiteboardId = request.getWhiteboardId();
        var fieldName = request.getFieldName();

        if (!accessManager.checkAccess(userId, whiteboardId)) {
            LOG.error("PERMISSION DENIED: <{}> trying to access whiteboard <{}> without permission",
                userId, whiteboardId);
            responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                "You don't have access to whiteboard <" + whiteboardId + ">").asException());
            return;
        }

        LOG.info("Link field {} of whiteboard {}", request.getFieldName(), request.getWhiteboardId());

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            // should already be completed
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, LinkFieldResponse.class,
                Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
        {
            return;
        }

        var operationId = UUID.randomUUID().toString();
        var response = LinkFieldResponse.getDefaultInstance();

        var op = Operation.createCompleted(
            operationId,
            userId,
            "Link whiteboard field: whiteboardId=%s, fieldName=%s".formatted(request.getWhiteboardId(),
                request.getFieldName()),
            idempotencyKey,
            /* meta */ null,
            response);

        var finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var linkedField = new LinkedField(fieldName, Field.Status.FINALIZED, request.getStorageUri(),
            fromProto(request.getScheme()));

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var tx = TransactionHandle.create(dataSource)) {
                    operationDao.create(op, tx);

                    Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, tx);

                    Field field = whiteboard.getField(fieldName);
                    if (field == null) {
                        throw new NotFoundException("Field " + fieldName + " of whiteboard " + whiteboardId +
                            " not found");
                    }
                    if (field.status() == Field.Status.FINALIZED) {
                        throw new IllegalArgumentException("Field " + fieldName + " of whiteboard " + whiteboardId +
                            " already finalized");
                    }
                    if (field instanceof LinkedField oldLinkedField) {
                        LOG.info("Field {} of whiteboard {} has already linked with data [{}]. " +
                            "Data will be overwritten.", fieldName, whiteboardId, oldLinkedField.toString());
                    }

                    whiteboardStorage.updateField(whiteboardId, linkedField, finalizedAt, tx);

                    tx.commit();
                }
            });

            responseObserver.onNext(LinkFieldResponse.getDefaultInstance());
            LOG.info("Finalize field {} of whiteboard {} done", fieldName, whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Finalize field {} of whiteboard {} failed, not found exception: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Finalize field {} of whiteboard {} failed, invalid argument: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            // should already be completed
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, LinkFieldResponse.class, Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
            {
                return;
            }

            LOG.error("Cannot create operation of whiteboard field linkage: { whiteboardId: {}, fieldName: {} }, " +
                "error: {}", request.getWhiteboardId(), request.getFieldName(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void finalizeWhiteboard(FinalizeWhiteboardRequest request,
                                   StreamObserver<FinalizeWhiteboardResponse> responseObserver)
    {
        final var authenticationContext = AuthenticationContext.current();
        final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

        final String whiteboardId = request.getWhiteboardId();
        if (whiteboardId.isBlank()) {
            throw new IllegalArgumentException("Request shouldn't contain empty fields");
        }

        if (!accessManager.checkAccess(userId, whiteboardId)) {
            LOG.error("PERMISSION DENIED: <{}> trying to access whiteboard <{}> without permission",
                userId, whiteboardId);
            responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                "You don't have access to whiteboard <" + whiteboardId + ">").asException());
            return;
        }

        LOG.info("Finalize whiteboard {}", request.getWhiteboardId());

        final Instant finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            // should already be completed
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, FinalizeWhiteboardResponse.class,
                Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
        {
            return;
        }

        var operationId = UUID.randomUUID().toString();
        var response = FinalizeWhiteboardResponse.getDefaultInstance();

        var op = Operation.createCompleted(
            operationId,
            userId,
            "Finalize whiteboard: whiteboardId=" + request.getWhiteboardId(),
            idempotencyKey,
            /* meta */ null,
            response);

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var tx = TransactionHandle.create(dataSource)) {
                    operationDao.create(op, tx);

                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, tx);

                    if (whiteboard.status() == Whiteboard.Status.FINALIZED) {
                        throw new IllegalArgumentException("Whiteboard " + whiteboardId + " already finalized");
                    }

                    final var unlinkedFields = whiteboard.unlinkedFields();
                    if (!unlinkedFields.isEmpty()) {
                        LOG.info("Finalize whiteboard {} with unlinked fields: [{}]",
                            whiteboardId, String.join(",", unlinkedFields.toString()));
                    }

                    whiteboardStorage.finalizeWhiteboard(whiteboardId, finalizedAt, tx);

                    tx.commit();
                }
            });

            responseObserver.onNext(FinalizeWhiteboardResponse.getDefaultInstance());
            LOG.info("Finalize whiteboard {} done", whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Finalize whiteboard {} failed, not found exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Finalize whiteboard {} failed, invalid argument: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            // should already be completed
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, FinalizeWhiteboardResponse.class, Duration.ofMillis(0), Duration.ofSeconds(0), LOG))
            {
                return;
            }

            LOG.error("Cannot create operation of whiteboard finalizing: { whiteboardId: {} }, " +
                "error: {}", request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    private static boolean validateCreateRequest(CreateWhiteboardRequest request,
                                                 StreamObserver<CreateWhiteboardResponse> responseObserver)
    {
        var methodName = "Create whiteboard";

        if (request.getWhiteboardName().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank whiteboard name", responseObserver);
        }

        if (request.getFieldsCount() == 0) {
            return replyValidateError(methodName, "Request must contain at least one field", responseObserver);
        }

        if (request.getNamespace().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank namespace", responseObserver);
        }

        if (request.getStorage().getName().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank storage name", responseObserver);
        }

        if (request.getFieldsList().stream().anyMatch(field -> !isValid(field))) {
            return replyValidateError(methodName, "Request contains invalid whiteboard field", responseObserver);
        }

        return true;
    }

    private static boolean validateLinkRequest(LinkFieldRequest request, StreamObserver<LinkFieldResponse> response) {
        var methodName = "Link whiteboard field";

        if (request.getWhiteboardId().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank whiteboard id", response);
        }

        if (request.getFieldName().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank field name", response);
        }

        if (request.getStorageUri().isBlank()) {
            return replyValidateError(methodName, "Request must contain not blank storage uri", response);
        }

        if (!isValid(request.getScheme())) {
            return replyValidateError(methodName, "Request must contain valid data schema", response);
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
