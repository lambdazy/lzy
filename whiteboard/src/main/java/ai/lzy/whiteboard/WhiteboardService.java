package ai.lzy.whiteboard;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.ReadMode;
import ai.lzy.model.db.TransactionHandleImpl;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.LWBS;
import ai.lzy.v1.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import ai.lzy.whiteboard.util.GrpcConverter;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WhiteboardService extends LzyWhiteboardServiceGrpc.LzyWhiteboardServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final WhiteboardStorage whiteboardStorage;
    private final WhiteboardDataSource dataSource;

    @Inject
    public WhiteboardService(WhiteboardStorage whiteboardStorage, WhiteboardDataSource dataSource) {
        this.whiteboardStorage = whiteboardStorage;
        this.dataSource = dataSource;
    }

    @Override
    public void create(LWBS.CreateRequest request, StreamObserver<LWBS.Whiteboard> responseObserver) {
        LOG.info("Create whiteboard: {}", JsonUtils.printRequest(request));

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

            if (!isRequestValid(request)) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }
            if (request.getFieldNamesList().stream().anyMatch(String::isBlank)) {
                throw new IllegalArgumentException("Field shouldn't be empty");
            }

            final String whiteboardId = "whiteboard-" + UUID.randomUUID();
            final Instant createdAt = Instant.now();
            final Whiteboard whiteboard = new Whiteboard(whiteboardId,
                new HashSet<>(request.getFieldNamesList()), Set.of(), new HashSet<>(request.getTagsList()),
                new Whiteboard.Storage(request.getStorage().getName(), request.getStorage().getDescription()),
                request.getNamespace(), Whiteboard.Status.CREATED, createdAt);

            whiteboardStorage.insertWhiteboard(userId, whiteboard, null);

            responseObserver.onNext(GrpcConverter.to(whiteboard));
            LOG.info("Create whiteboard done, id = {}", whiteboardId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Create whiteboard failed, invalid argument: {}", e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Create whiteboard failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void linkField(LWBS.LinkFieldRequest request, StreamObserver<LWBS.LinkFieldResponse> responseObserver) {
        LOG.info("Link field {} to whiteboard {}", request.getFieldName(), request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String whiteboardId = request.getWhiteboardId();
            final String fieldName = request.getFieldName();

            if (!isRequestValid(request)) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }

            final Instant linkedAt = Instant.now();
            final var linkedField = new Whiteboard.LinkedField(fieldName, request.getStorageUri(),
                ai.lzy.model.GrpcConverter.contentTypeFrom(request.getScheme()));

            try (final var transaction = new TransactionHandleImpl(dataSource)) {
                final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(
                    userId, whiteboardId, transaction, ReadMode.FOR_UPDATE
                );

                if (!whiteboard.hasField(fieldName)) {
                    throw new NotFoundException("Field " + fieldName + " of whiteboard " + whiteboard + " not found");
                }
                if (whiteboard.linkedFields().stream().anyMatch(f -> f.name().contains(fieldName))) {
                    throw new IllegalArgumentException(
                        "Field " + fieldName + " of whiteboard " + whiteboardId + " already linked"
                    );
                }

                whiteboardStorage.linkField(userId, whiteboardId, fieldName, linkedField, linkedAt, transaction);
            }

            responseObserver.onNext(LWBS.LinkFieldResponse.getDefaultInstance());
            LOG.info("Link field {} to whiteboard {} done", fieldName, whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Link field {} to whiteboard {} failed, not found exception: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Link field {} to whiteboard {} failed, invalid argument: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Link field {} to whiteboard {} failed, got exception: {}",
                request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void finalize(LWBS.FinalizeRequest request, StreamObserver<LWBS.FinalizeResponse> responseObserver) {
        LOG.info("Finalize whiteboard {}", request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String whiteboardId = request.getWhiteboardId();

            if (whiteboardId.isBlank()) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }

            final Instant finalizedAt = Instant.now();
            try (final var transaction = new TransactionHandleImpl(dataSource)) {
                final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(
                    userId, whiteboardId, transaction, ReadMode.FOR_UPDATE
                );

                if (!whiteboard.createdFieldNames().isEmpty()) {
                    String errorMessage = String.format(
                        "Finalize whiteboard %s failed, whiteboard has unlinked fields: %s",
                        whiteboardId, String.join(",", whiteboard.createdFieldNames())
                    );
                    LOG.error(errorMessage);
                    responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(errorMessage).asException());
                    return;
                }

                whiteboardStorage.setWhiteboardFinalized(userId, whiteboardId, finalizedAt, transaction);
            }

            responseObserver.onNext(LWBS.FinalizeResponse.getDefaultInstance());
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
        }  catch (Exception e) {
            LOG.error("Finalize whiteboard {} failed, got exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void get(LWBS.GetRequest request, StreamObserver<LWBS.Whiteboard> responseObserver) {
        LOG.info("Get whiteboard {}", request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String whiteboardId = request.getWhiteboardId();

            if (whiteboardId.isBlank()) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }

            final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(
                userId, whiteboardId, null, ReadMode.DEFAULT
            );

            responseObserver.onNext(GrpcConverter.to(whiteboard));
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

            Stream<Whiteboard> whiteboards = whiteboardStorage.listWhiteboards(
                userId, request.getTagsList(), null, ReadMode.DEFAULT
            );

            Stream<Whiteboard> filtered = switch (request.getQueryCase()) {
                case DATETIMEQUERY -> {
                    final var dateTimeQuery = request.getDateTimeQuery();
                    if (!isQueryValid(dateTimeQuery)) {
                        throw new IllegalArgumentException("Request shouldn't contain empty fields");
                    }

                    yield whiteboards
                        .filter(wb -> wb.createdAt().isAfter(ai.lzy.model.GrpcConverter.from(dateTimeQuery.getFrom())))
                        .filter(wb -> wb.createdAt().isBefore(ai.lzy.model.GrpcConverter.from(dateTimeQuery.getTo())));
                }
                default -> throw new IllegalArgumentException("Incorrect query type " + request.getQueryCase().name());
            };

            var response = LWBS.ListResponse.newBuilder()
                .addAllWhiteboards(filtered.map(GrpcConverter::to).toList())
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

    private boolean isRequestValid(LWBS.CreateRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && request.getFieldNamesCount() != 0;
            isValid = isValid && !request.getNamespace().isBlank();
            isValid = isValid && !request.getStorage().getName().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    private boolean isRequestValid(LWBS.LinkFieldRequest request) {
        try {
            boolean isValid = true;
            isValid = isValid && !request.getWhiteboardId().isBlank();
            isValid = isValid && !request.getFieldName().isBlank();
            isValid = isValid && !request.getStorageUri().isBlank();
            isValid = isValid && !request.getScheme().getType().isBlank();
            isValid = isValid && request.getScheme().getSchemeType().getNumber() != 0;
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

    private boolean isQueryValid(LWBS.DateTimeQuery query) {
        try {
            boolean isValid = true;
            isValid = isValid && query.hasFrom();
            isValid = isValid && query.hasTo();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }
}
