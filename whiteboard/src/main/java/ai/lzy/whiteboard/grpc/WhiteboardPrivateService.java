package ai.lzy.whiteboard.grpc;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.LWBPS;
import ai.lzy.v1.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.whiteboard.access.AccessManager;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.LinkedField;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;

public class WhiteboardPrivateService extends LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardPrivateService.class);

    private final AccessManager accessManager;
    private final WhiteboardStorage whiteboardStorage;
    private final WhiteboardDataSource dataSource;
    private final LockManager lockManager;

    @Inject
    public WhiteboardPrivateService(AccessManager accessManager,
                                    WhiteboardStorage whiteboardStorage,
                                    WhiteboardDataSource dataSource)
    {
        this.accessManager = accessManager;
        this.whiteboardStorage = whiteboardStorage;
        this.dataSource = dataSource;
        this.lockManager = new LocalLockManager().withPrefix("whiteboard");
    }

    @Override
    public void createWhiteboard(LWBPS.CreateWhiteboardRequest request,
                       StreamObserver<LWBPS.CreateWhiteboardResponse> responseObserver)
    {
        LOG.info("Create whiteboard {}", request.getWhiteboardName());

        try {
            if (!ProtoValidator.isValid(request)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Create whiteboard failed, invalid argument: {}", errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            final String whiteboardId = "whiteboard-" + UUID.randomUUID();
            final Instant createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            final Map<String, Field> fields = request.getFieldsList().stream()
                .map(ProtoConverter::fromProto)
                .collect(Collectors.toMap(Field::name, x -> x));
            final Whiteboard whiteboard = new Whiteboard(whiteboardId, request.getWhiteboardName(), fields,
                new HashSet<>(request.getTagsList()), ProtoConverter.fromProto(request.getStorage()),
                request.getNamespace(), Whiteboard.Status.CREATED, createdAt);

            final var lock = lockManager.getOrCreate(whiteboardId);
            withRetries(defaultRetryPolicy(), LOG, () -> {
                lock.lock();
                try {
                    whiteboardStorage.insertWhiteboard(request.getUserId(), whiteboard, null);
                } finally {
                    lock.unlock();
                }
            });

            try {
                accessManager.addAccess(request.getUserId(), whiteboardId);
            } catch (Exception e) {
                String errorMessage = "Failed to get access to whiteboard for user " + request.getUserId();
                LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(), errorMessage, e);
                LOG.info("Undo creating whiteboard {}, id = {}", request.getWhiteboardName(), whiteboardId);
                withRetries(defaultRetryPolicy(), LOG, () -> {
                    lock.lock();
                    try {
                        whiteboardStorage.deleteWhiteboard(whiteboardId, null);
                    } finally {
                        lock.unlock();
                    }
                });
                LOG.info("Undo creating whiteboard {} done", request.getWhiteboardName());
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }

            responseObserver.onNext(LWBPS.CreateWhiteboardResponse.newBuilder()
                .setWhiteboard(ProtoConverter.toProto(whiteboard))
                .build());
            LOG.info("Create whiteboard {} done, id = {}", request.getWhiteboardName(), whiteboardId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Create whiteboard {} failed, invalid argument: {}", request.getWhiteboardName(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void finalizeField(LWBPS.FinalizeFieldRequest request,
                              StreamObserver<LWBPS.FinalizeFieldResponse> responseObserver)
    {
        LOG.info("Finalize field {} of whiteboard {}", request.getFieldName(), request.getWhiteboardId());

        try {
            final String whiteboardId = request.getWhiteboardId();
            final String fieldName = request.getFieldName();

            if (!ProtoValidator.isValid(request)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Finalize field {} of whiteboard {} failed, invalid argument: {}",
                    request.getFieldName(), request.getWhiteboardId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            }

            final Instant finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            final var linkedField = new LinkedField(fieldName, Field.Status.FINALIZED, request.getStorageUri(),
                ai.lzy.model.GrpcConverter.contentTypeFrom(request.getScheme()));

            withRetries(defaultRetryPolicy(), LOG, () -> {
                final var lock = lockManager.getOrCreate(whiteboardId);
                lock.lock();
                try (final var transaction = TransactionHandle.create(dataSource)) {
                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, transaction);

                    final Field field = whiteboard.getField(fieldName);
                    if (field == null) {
                        throw new NotFoundException("Field " + fieldName + " of whiteboard " + whiteboardId + " not found");
                    }
                    if (field.status() == Field.Status.FINALIZED) {
                        throw new IllegalArgumentException("Field " + fieldName + " of whiteboard " + whiteboardId + " already finalized");
                    }
                    if (field instanceof LinkedField oldLinkedField) {
                        LOG.info("Field {} of whiteboard {} has already linked with data [{}]. " +
                                 "Data will be overwritten.", fieldName, whiteboardId, oldLinkedField.toString());
                    }

                    whiteboardStorage.updateField(whiteboardId, linkedField, finalizedAt, transaction);

                    transaction.commit();
                } finally {
                    lock.unlock();
                }
            });

            responseObserver.onNext(LWBPS.FinalizeFieldResponse.getDefaultInstance());
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
            LOG.error("Finalize field {} of whiteboard {} failed, got exception: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void finalizeWhiteboard(LWBPS.FinalizeWhiteboardRequest request,
                         StreamObserver<LWBPS.FinalizeWhiteboardResponse> responseObserver)
    {
        LOG.info("Finalize whiteboard {}", request.getWhiteboardId());

        try {
            final String whiteboardId = request.getWhiteboardId();
            if (whiteboardId.isBlank()) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }

            final Instant finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (final var transaction = TransactionHandle.create(dataSource)) {
                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, transaction);

                    if (whiteboard.status() == Whiteboard.Status.FINALIZED) {
                        throw new IllegalArgumentException("Whiteboard " + whiteboardId + " already finalized");
                    }

                    final var unlinkedFields = whiteboard.unlinkedFields();
                    if (!unlinkedFields.isEmpty()) {
                         LOG.info("Finalize whiteboard {} with unlinked fields: [{}]",
                             whiteboardId, String.join(",", unlinkedFields.toString()));
                    }

                    whiteboardStorage.finalizeWhiteboard(whiteboardId, finalizedAt, transaction);

                    transaction.commit();
                }
            });

            responseObserver.onNext(LWBPS.FinalizeWhiteboardResponse.getDefaultInstance());
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
            LOG.error("Finalize whiteboard {} failed, got exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

}
