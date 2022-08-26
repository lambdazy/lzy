package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.TransactionHandleImpl;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.VmAllocatorApi.*;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.List;

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

        final Session session;
        try {
            session = sessions.create(request.getOwner(), policy, null);
        } catch (Exception e) {
            LOG.error("Cannot create session: {}", e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(CreateSessionResponse.newBuilder()
            .setSessionId(session.sessionId())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        try (var transaction = TransactionHandle.create(storage)) {
            final List<Vm> vms = dao.list(request.getSessionId(), transaction);
            final var now = Instant.now();
            vms.forEach(vm ->
                dao.update(Vm.from(vm)
                    .setDeadline(now)
                    .setState(Vm.State.IDLE)
                    .build(),
                transaction
            ));
            sessions.delete(request.getSessionId(), transaction);
            transaction.commit();
        } catch (Exception e) {
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }


    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        LOG.info("Allocation request {}", JsonUtils.printSingleLine(request));

        final var startedAt = Instant.now();

        final Session session;
        try {
            session = sessions.get(request.getSessionId(), null);
        } catch (Exception e) {
            LOG.error("Cannot get session {}: {}", request.getSessionId(), e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        if (session == null) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        var op = operations.create(
            "Allocating VM",
            session.owner(),
            Any.pack(AllocateMetadata.getDefaultInstance()),
            null);

        Vm vm;
        try (var transaction = TransactionHandle.create(storage)) {
            final var existingVm = dao.acquire(request.getSessionId(), request.getPoolLabel(),
                request.getZone(), transaction);

            if (existingVm != null) {
                LOG.info("Found existing VM {}", existingVm);

                op = op.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                    .setVmId(existingVm.vmId())
                    .build()));
                op = op.complete(Any.pack(AllocateResponse.newBuilder()
                    .setSessionId(existingVm.sessionId())
                    .setPoolId(existingVm.poolLabel())
                    .setVmId(existingVm.vmId())
                    .putAllMetadata(existingVm.vmMeta())
                    .build()));
                operations.update(op, transaction);

                dao.update(
                    Vm.from(existingVm)
                        .setState(Vm.State.RUNNING)
                        .build(),
                    transaction);

                transaction.commit();

                metrics.allocateVmExisting.inc();

                responseObserver.onNext(op.toProto());
                responseObserver.onCompleted();
                return;
            }

            var workloads = request.getWorkloadList().stream()
                .map(Workload::fromGrpc)
                .toList();

            vm = dao.create(request.getSessionId(), request.getPoolLabel(), request.getZone(), workloads, op.id(),
                startedAt, transaction);

            op = op.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                .setVmId(vm.vmId())
                .build()));

            operations.update(op, transaction);

            vm = Vm.from(vm)
                .setState(Vm.State.CONNECTING)
                .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                .build();
            dao.update(vm, transaction);

            transaction.commit();

            metrics.allocateVmNew.inc();

            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("Error while executing transaction", e);
            op = op.complete(Status.INTERNAL.withDescription(e.getMessage()));
            operations.update(op, null);

            metrics.allocationError.inc();

            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
            return;
        }

        try {
            try {
                var timer = metrics.allocateDuration.startTimer();
                allocator.allocate(vm);
                timer.close();
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating: {}", e.getMessage(), e);
                metrics.allocationError.inc();
                operations.update(op.complete(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            operations.update(op.complete(Status.INTERNAL.withDescription("Error while executing request")), null);
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        try (var transaction = TransactionHandle.create(storage)) {
            var vm = dao.get(request.getVmId(), transaction);
            if (vm == null) {
                responseObserver.onError(
                    Status.NOT_FOUND.withDescription("Cannot find vm").asException());
                return;
            }

            // TODO(artolord) validate that client can free this vm
            if (vm.state() != Vm.State.RUNNING) {
                LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            var session = sessions.get(vm.sessionId(), transaction);
            if (session == null) {
                LOG.error("Corrupted vm with incorrect session id: {}", vm);
                responseObserver.onError(Status.INTERNAL.asException());
                return;
            }

            dao.update(
                Vm.from(vm)
                    .setState(Vm.State.IDLE)
                    .setDeadline(Instant.now().plus(session.cachePolicy().minIdleTimeout()))
                    .build(),
                transaction);

            transaction.commit();
        } catch (Exception e) {
            LOG.error("Error while freeing", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while freeing").asException());
            return;
        }

        responseObserver.onNext(FreeResponse.getDefaultInstance());
        responseObserver.onCompleted();
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
