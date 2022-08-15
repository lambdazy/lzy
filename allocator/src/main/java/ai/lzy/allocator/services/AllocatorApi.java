package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
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
        VmAllocator allocator, ServiceConfig config, Storage storage) {
        this.dao = dao;
        this.operations = operations;
        this.sessions = sessions;
        this.allocator = allocator;
        this.config = config;
        this.storage = storage;
    }

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        final var minIdleTimeout = Duration.ofSeconds(
            request.getCachePolicy().getIdleTimeout().getSeconds())
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
        responseObserver.onNext(DeleteSessionResponse.newBuilder().build());
        responseObserver.onCompleted();
        try (var transaction = new TransactionHandle(storage)) {
            final List<Vm> vms = dao.list(request.getSessionId(), transaction);
            vms.forEach(vm -> {
                dao.update(new Vm.VmBuilder(vm)
                    .setState(Vm.State.DEAD)
                    .build(),
                    transaction
                );
                allocator.deallocate(vm);
            });
            sessions.delete(request.getSessionId(), transaction);
            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error while executing request", e);
        }
    }


    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
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

        try (var transaction = new TransactionHandle(storage)) {

            final var existingVm = dao.acquire(request.getSessionId(), request.getPoolLabel(),
                request.getZone(), transaction);

            if (existingVm != null) {
                op = op.complete(Any.pack(AllocateResponse.newBuilder()
                    .setSessionId(existingVm.sessionId())
                    .setPoolId(existingVm.poolLabel())
                    .setVmId(existingVm.poolLabel())
                    .putAllMetadata(existingVm.vmMeta())
                    .build()));
                operations.update(op, transaction);
                responseObserver.onNext(op.toGrpc());
                responseObserver.onCompleted();
                return;
            }
            transaction.commit();
        } catch (Exception e) {
            LOG.error("Error while executing transaction", e);
            op = op.complete(Status.INTERNAL.withDescription("Error while executing request"));
            operations.update(op, null);
            responseObserver.onNext(op.toGrpc());
            responseObserver.onCompleted();
            return;
        }
        responseObserver.onNext(op.toGrpc());
        responseObserver.onCompleted();

        var workloads = request.getWorkloadList().stream()
            .map(Workload::fromGrpc)
            .toList();

        Vm vm = null;

        try (var transaction = new TransactionHandle(storage)) {
            vm = dao.create(request.getSessionId(), request.getPoolLabel(),
                request.getZone(), workloads, op.id(), transaction);
            op = op.modifyMeta(Any.pack(AllocateMetadata.newBuilder().setVmId(vm.vmId()).build()));
            operations.update(op, transaction);

            allocator.allocate(vm, transaction);

            vm = new Vm.VmBuilder(vm)
                .setState(Vm.State.CONNECTING)
                .setAllocationDeadline(Instant.now().plus(config.allocationTimeout()))  // TODO(artolord) add to config
                .build();
            dao.update(vm, transaction);
            transaction.commit();
        } catch (Exception e) {
            LOG.error("Error while executing request", e);
            if (vm != null) {
                allocator.deallocate(vm);
                operations.update(op.complete(Status.INTERNAL.withDescription("Error while executing request")), null);
            }
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        var vm = dao.get(request.getVmId(), null);
        if (vm == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot found vm").asException());
            return;
        }
        // TODO(artolord) validate that client can free this vm
        if (vm.state() != Vm.State.RUNNING) {
            LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }
        var session = sessions.get(vm.sessionId(), null);
        if (session == null) {
            LOG.error("Corrupted vm with incorrect session id");
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }

        dao.update(new Vm.VmBuilder(vm)
            .setState(Vm.State.IDLE)
            .setDeadline(Instant.now().plus(session.cachePolicy().minIdleTimeout()))
            .build(), null);

        responseObserver.onNext(FreeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
