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
import io.micronaut.retry.annotation.Retryable;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorApi extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final SessionDao sessions;
    private final VmAllocator allocator;
    private final ServiceConfig config;
    private final AllocatorDataSource storage;
    private final Metrics metrics = new Metrics();
    private final AllocatorImpl allocatorImpl = new AllocatorImpl();

    @Inject
    public AllocatorApi(VmDao dao, OperationDao operations, SessionDao sessions, VmAllocator allocator,
                        ServiceConfig config, AllocatorDataSource storage)
    {
        this.dao = dao;
        this.operations = operations;
        this.sessions = sessions;
        this.allocator = allocator;
        this.config = config;
        this.storage = storage;
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
            operation = allocatorImpl.createOperation(session.owner());
        } catch (SQLException e) {
            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        final Vm.Spec vmSpec;
        try {
            vmSpec = allocatorImpl.createVmSpec(request, startedAt, operation);
            responseObserver.onNext(operation.toProto());
            responseObserver.onCompleted();
        } catch (SQLException e) {
            final String exceptionMessage = e.getMessage();
            LOG.error("Error while executing transaction: {}", exceptionMessage, e);
            failOperation(operation, exceptionMessage);
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
                operations.update(operation.complete(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            failOperation(operation, "Error while executing request");
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

    private final class AllocatorImpl {
        @Retryable
        Session createSession(String owner, CachePolicy cachePolicy) throws SQLException {
            return sessions.create(owner, cachePolicy, null);
        }

        @Retryable
        public void deleteSession(String sessionId) throws SQLException {
            sessions.delete(sessionId, null);
            dao.delete(sessionId);
        }

        @Nullable
        @Retryable
        public Session getSession(String sessionId) throws SQLException {
            return sessions.get(sessionId, null);
        }

        @Retryable
        public Operation createOperation(String owner) throws SQLException {
            return operations.create(UUID.randomUUID().toString(), "Allocating VM", owner,
                Any.pack(AllocateMetadata.getDefaultInstance()), null);
        }

        @Retryable
        public Vm.Spec createVmSpec(AllocateRequest request, Instant startedAt, Operation operation)
            throws SQLException
        {
            try (var transaction = TransactionHandle.create(storage)) {
                final var existingVm = dao.acquire(request.getSessionId(),
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
                    operations.update(operation, transaction);

                    transaction.commit();

                    metrics.allocateVmExisting.inc();
                    return existingVm.spec();
                }

                var workloads = request.getWorkloadList().stream()
                    .map(Workload::fromProto)
                    .toList();
                final var volumes = request.getVolumesList().stream()
                    .map(VolumeRequest::fromProto)
                    .toList();

                var vmSpec = dao.create(request.getSessionId(), request.getPoolLabel(), request.getZone(),
                    workloads, volumes, operation.id(), startedAt, transaction);

                operation = operation.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                    .setVmId(vmSpec.vmId())
                    .build()));

                operations.update(operation, transaction);

                final var vmState = new Vm.VmStateBuilder()
                    .setStatus(Vm.VmStatus.CONNECTING)
                    .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                    .build();
                dao.update(vmSpec.vmId(), vmState, transaction);

                transaction.commit();
                metrics.allocateVmNew.inc();

                return vmSpec;
            }
        }

        @Retryable
        public Status free(String vmId) throws SQLException {
            try (var transaction = TransactionHandle.create(storage)) {
                var vm = dao.get(vmId, transaction);
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

                dao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()), transaction);

                transaction.commit();
                return Status.OK;
            }
        }
    }

    @Retryable
    private void failOperation(Operation operation, String exceptionMessage) {
        try {
            operation = operation.complete(Status.INTERNAL.withDescription(exceptionMessage));
            operations.update(operation, null);
        } catch (SQLException ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}", operation, exceptionMessage, ex.getMessage(), ex);
        }
    }

    private static final class Metrics {
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
