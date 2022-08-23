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
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.VmAllocatorApi.*;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Singleton
public class AllocatorApi extends AllocatorGrpc.AllocatorImplBase {

    private static final Logger LOG = LogManager.getLogger(AllocatorApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final SessionDao sessions;
    private final VmAllocator allocator;
    private final ServiceConfig config;
    private final Storage storage;

    @Inject
    public AllocatorApi(VmDao dao, OperationDao operations, SessionDao sessions,
                        VmAllocator allocator, ServiceConfig config, AllocatorDataSource storage) {
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
        } else if (!request.hasCachePolicy() || !request.getCachePolicy().hasIdleTimeout()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Cache policy is not properly set").asRuntimeException());
            return;
        }

        final var minIdleTimeout = Duration.ofSeconds(request.getCachePolicy().getIdleTimeout().getSeconds())
            .plus(request.getCachePolicy().getIdleTimeout().getNanos(), ChronoUnit.NANOS);
        final var policy = new CachePolicy(minIdleTimeout);

        final Session session = sessions.create(request.getOwner(), policy, null);
        responseObserver.onNext(CreateSessionResponse.newBuilder()
            .setSessionId(session.sessionId())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        try (var transaction = new TransactionHandle(storage)) {
            final List<Vm> vms = dao.list(request.getSessionId(), transaction);
            vms.forEach(vm -> dao.update(new Vm.VmBuilder(vm)
                    .setDeadline(Instant.now())
                    .setState(Vm.State.IDLE)
                    .build(),
                transaction
            ));
            sessions.delete(request.getSessionId(), transaction);
            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error while executing request", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while executing request").asException());
            return;
        }
        responseObserver.onNext(DeleteSessionResponse.newBuilder().build());
        responseObserver.onCompleted();
    }


    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        LOG.info("Allocation request {}", JsonUtils.printRequest(request));
        final Session session = sessions.get(request.getSessionId(), null);
        if (session == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        var op = operations.create(
            "Allocating vm",
            session.owner(),
            Any.pack(AllocateMetadata.newBuilder().build()),
            null
        );
        Vm vm;
        try (var transaction = new TransactionHandle(storage)) {
            final var existingVm = dao.acquire(request.getSessionId(), request.getPoolLabel(),
                request.getZone(), transaction);
            if (existingVm != null) {
                LOG.info("Found existing VM {}", existingVm);
                op = op.modifyMeta(Any.pack(AllocateMetadata.newBuilder().setVmId(existingVm.vmId()).build()));
                op = op.complete(Any.pack(AllocateResponse.newBuilder()
                    .setSessionId(existingVm.sessionId())
                    .setPoolId(existingVm.poolLabel())
                    .setVmId(existingVm.vmId())
                    .putAllMetadata(existingVm.vmMeta())
                    .build()));
                operations.update(op, transaction);
                dao.update(new Vm.VmBuilder(existingVm).setState(Vm.State.RUNNING).build(), transaction);
                transaction.commit();
                responseObserver.onNext(op.toGrpc());
                responseObserver.onCompleted();
                return;
            }

            var workloads = request.getWorkloadList().stream()
                .map(Workload::fromGrpc)
                .toList();
            vm = dao.create(request.getSessionId(), request.getPoolLabel(),
                request.getZone(), workloads, op.id(), transaction);
            op = op.modifyMeta(Any.pack(AllocateMetadata.newBuilder().setVmId(vm.vmId()).build()));
            operations.update(op, transaction);

            vm = new Vm.VmBuilder(vm)
                .setState(Vm.State.CONNECTING)
                .setAllocationDeadline(
                    Instant.now().plus(config.getAllocationTimeout()))
                .build();
            dao.update(vm, transaction);

            transaction.commit();
            responseObserver.onNext(op.toGrpc());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("Error while executing transaction", e);
            op = op.complete(Status.INTERNAL.withDescription("Error while executing request"));
            operations.update(op, null);
            responseObserver.onNext(op.toGrpc());
            responseObserver.onCompleted();
            return;
        }

        try {
            try {
                allocator.allocate(vm);
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating", e);
                operations.update(op.complete(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation", e);
            operations.update(op.complete(Status.INTERNAL.withDescription("Error while executing request")), null);
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        try (var transaction = new TransactionHandle(storage)) {
            var vm = dao.get(request.getVmId(), transaction);
            if (vm == null) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot found vm").asException());
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
                LOG.error("Corrupted vm with incorrect session id");
                responseObserver.onError(Status.INTERNAL.asException());
                return;
            }

            dao.update(new Vm.VmBuilder(vm)
                .setState(Vm.State.IDLE)
                .setDeadline(Instant.now().plus(session.cachePolicy().minIdleTimeout()))
                .build(), transaction);
            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error while freeing", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while freeing").asException());
            return;
        }
        responseObserver.onNext(FreeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
