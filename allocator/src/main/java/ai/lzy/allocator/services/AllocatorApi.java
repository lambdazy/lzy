package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Operation;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.volume.VolumeRequest;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionRequest;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionResponse;
import ai.lzy.v1.VmAllocatorApi.FreeRequest;
import ai.lzy.v1.VmAllocatorApi.FreeResponse;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.micronaut.retry.annotation.RetryPredicate;
import io.micronaut.retry.annotation.Retryable;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorApi extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorApi.class);

    private final VmAllocator allocator;
    private final Metrics metrics;
    private final AllocatorImpl allocatorImpl;
    private final RetryableOperation retryableOperation;

    @Inject
    public AllocatorApi(AllocatorImpl allocatorImpl, RetryableOperation retryableOperation,
                        VmAllocator allocator, Metrics metrics)
    {
        this.allocator = allocator;
        this.allocatorImpl = allocatorImpl;
        this.retryableOperation = retryableOperation;
        this.metrics = metrics;
    }

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        if (request.getOwner().isBlank()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Owner is not provided").asRuntimeException());
            return;
        }

        if (!request.hasCachePolicy() || !request.getCachePolicy().hasIdleTimeout()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Cache policy is not properly set").asRuntimeException());
            return;
        }

        final var minIdleTimeout = ProtoConverter.fromProto(request.getCachePolicy().getIdleTimeout());
        final var policy = new CachePolicy(minIdleTimeout);

        try {
            final var session = allocatorImpl.createSession(request.getOwner(), policy);
            responseObserver.onNext(CreateSessionResponse.newBuilder()
                .setSessionId(session.sessionId())
                .build());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            LOG.error("Cannot create session: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        try {
            allocatorImpl.deleteSession(request.getSessionId());
            responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<OperationService.Operation> responseObserver) {
        LOG.info("Allocation request {}", JsonUtils.printSingleLine(request));

        final var startedAt = Instant.now();

        final Session session;
        try {
            session = allocatorImpl.getSession(request.getSessionId());
            if (session == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
                return;
            }
        } catch (SQLException e) {
            LOG.error("Cannot get session {}: {}", request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }


        Operation operation;
        try {
            operation = retryableOperation.createOperation(session.owner());
        } catch (SQLException e) {
            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        final Vm.Spec vmSpec;
        try {
            vmSpec = allocatorImpl.createVmSpec(request, startedAt, operation);
            operation = retryableOperation.getOperation(operation.id());
            responseObserver.onNext(operation.toProto());
            responseObserver.onCompleted();
            if (vmSpec == null) {
                return;
            }
        } catch (SQLException e) {
            final String exceptionMessage = e.getMessage();
            LOG.error("Error while executing transaction: {}", exceptionMessage, e);
            retryableOperation.failOperation(operation, exceptionMessage);
            metrics.allocationError.inc();

            responseObserver.onNext(operation.toProto());
            responseObserver.onCompleted();
            return;
        }

        try {
            try {
                var timer = metrics.allocateDuration.startTimer();
                allocator.allocate(vmSpec);
                timer.close();
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating: {}", e.getMessage(), e);
                metrics.allocationError.inc();
                retryableOperation.update(
                    operation.complete(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            retryableOperation.failOperation(operation, "Error while executing request");
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        try {
            var result = allocatorImpl.free(request.getVmId());
            if (Status.Code.OK.equals(result.getCode())) {
                responseObserver.onNext(FreeResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(result.asException());
            }
        } catch (SQLException e) {
            LOG.error("Error while free vm {}: {}", request.getVmId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while free").asException());
        }
    }

    public static class DbRetryPredicate implements RetryPredicate {
        @Override
        public boolean test(Throwable throwable) {
            if (throwable instanceof PSQLException) {
                return DbHelper.canRetry((PSQLException) throwable);
            }
            return false;
        }
    }

    @Singleton
    @Retryable(delay = "50ms", multiplier = "2.0",
        predicate = DbRetryPredicate.class, capturedException = PSQLException.class)
    public static class AllocatorImpl {
        private final VmDao vmDao;
        private final SessionDao sessions;
        private final AllocatorDataSource storage;
        private final RetryableOperation retryableOperation;
        private final ServiceConfig config;
        private final Metrics metrics;

        @Inject
        public AllocatorImpl(VmDao vmDao, SessionDao sessions, AllocatorDataSource storage,
                             RetryableOperation retryableOperation, ServiceConfig config, Metrics metrics)
        {
            this.vmDao = vmDao;
            this.sessions = sessions;
            this.storage = storage;
            this.retryableOperation = retryableOperation;
            this.config = config;
            this.metrics = metrics;
        }

        Session createSession(String owner, CachePolicy cachePolicy) throws SQLException {
            return sessions.create(owner, cachePolicy, null);
        }

        public void deleteSession(String sessionId) throws SQLException {
            sessions.delete(sessionId, null);
            vmDao.delete(sessionId);
        }

        @Nullable
        public Session getSession(String sessionId) throws SQLException {
            return sessions.get(sessionId, null);
        }

        public Vm.Spec createVmSpec(AllocateRequest request, Instant startedAt, Operation operation)
            throws SQLException
        {
            try (var transaction = TransactionHandle.create(storage)) {
                final var existingVm = vmDao.acquire(request.getSessionId(),
                    request.getPoolLabel(), request.getZone(), transaction);

                if (existingVm != null) {
                    LOG.info("Found existing VM {}", existingVm);

                    operation = operation.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                        .setVmId(existingVm.vmId())
                        .build()));
                    operation = operation.complete(Any.pack(AllocateResponse.newBuilder()
                        .setSessionId(existingVm.sessionId())
                        .setPoolId(existingVm.poolLabel())
                        .setVmId(existingVm.vmId())
                        .putAllMetadata(existingVm.vmMeta())
                        .build()));
                    retryableOperation.update(operation, transaction);

                    transaction.commit();

                    metrics.allocateVmExisting.inc();
                    return null;
                }

                var workloads = request.getWorkloadList().stream()
                    .map(Workload::fromProto)
                    .toList();
                final var volumes = request.getVolumesList().stream()
                    .map(VolumeRequest::fromProto)
                    .toList();

                var vmSpec = vmDao.create(request.getSessionId(), request.getPoolLabel(), request.getZone(),
                    workloads, volumes, operation.id(), startedAt, transaction);

                operation = operation.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                    .setVmId(vmSpec.vmId())
                    .build()));

                retryableOperation.update(operation, transaction);

                final var vmState = new Vm.VmStateBuilder()
                    .setStatus(Vm.VmStatus.CONNECTING)
                    .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                    .build();
                vmDao.update(vmSpec.vmId(), vmState, transaction);

                transaction.commit();
                metrics.allocateVmNew.inc();

                return vmSpec;
            }
        }

        public Status free(String vmId) throws SQLException {
            try (var transaction = TransactionHandle.create(storage)) {
                var vm = vmDao.get(vmId, transaction);
                if (vm == null) {
                    return Status.NOT_FOUND.withDescription("Cannot find vm");
                }

                // TODO(artolord) validate that client can free this vm
                if (vm.status() != Vm.VmStatus.RUNNING) {
                    LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
                    return Status.FAILED_PRECONDITION.withDescription("State is " + vm.state());
                }

                var session = sessions.get(vm.sessionId(), transaction);
                if (session == null) {
                    LOG.error("Corrupted vm with incorrect session id: {}", vm);
                    return Status.INTERNAL;
                }

                vmDao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()), transaction);

                transaction.commit();
                return Status.OK;
            }
        }
    }

    @Singleton
    @Retryable(delay = "50ms", multiplier = "2.0",
        predicate = DbRetryPredicate.class, capturedException = PSQLException.class)
    public static class RetryableOperation {
        private final OperationDao operations;

        @Inject
        public RetryableOperation(OperationDao operations) {
            this.operations = operations;
        }

        public Operation createOperation(String owner) throws SQLException {
            return operations.create(UUID.randomUUID().toString(), "Allocating VM", owner,
                Any.pack(AllocateMetadata.getDefaultInstance()), null);
        }

        public void failOperation(Operation operation, String exceptionMessage) {
            try {
                operation = operation.complete(Status.INTERNAL.withDescription(exceptionMessage));
                operations.update(operation, null);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {} with reason {}: {}",
                    operation, exceptionMessage, ex.getMessage(), ex);
            }
        }

        public void update(Operation operation, TransactionHandle tx) throws SQLException {
            operations.update(operation, tx);
        }

        public Operation getOperation(String id) throws SQLException {
            return operations.get(id, null);
        }
    }

    @Singleton
    public static final class Metrics {
        private final Counter allocateVmExisting = Counter
            .build("allocate_vm_existing", "Allocate VM from cache")
            .subsystem("allocator")
            .register();

        private final Counter allocateVmNew = Counter
            .build("allocate_vm_new", "Allocate new VM")
            .subsystem("allocator")
            .register();

        private final Counter allocationError = Counter
            .build("allocate_error", "Allocation errors")
            .subsystem("allocator")
            .register();

        private final Histogram allocateDuration = Histogram
            .build("allocate_time", "Allocate duration (sec)")
            .subsystem("allocator")
            .buckets(0.001, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0)
            .register();
    }
}
