package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
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

    @Inject
    public AllocatorApi(VmDao dao, OperationDao operations, SessionDao sessions,
            VmAllocator allocator, ServiceConfig config) {
        this.dao = dao;
        this.operations = operations;
        this.sessions = sessions;
        this.allocator = allocator;
        this.config = config;
    }

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        final Session session = sessions.create(
            request.getOwner(),
            Duration.ofSeconds(request.getMinIdleTimeout().getSeconds())
                .plus(request.getMinIdleTimeout().getNanos(), ChronoUnit.NANOS));
        responseObserver.onNext(CreateSessionResponse.newBuilder()
            .setSessionId(session.sessionId())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        responseObserver.onNext(DeleteSessionResponse.newBuilder().build());
        responseObserver.onCompleted();
        final List<Vm> vms = dao.list(request.getSessionId());
        vms.forEach(vm -> {
            dao.update(
                new Vm.VmBuilder(vm)
                    .setState(Vm.State.DEAD)
                    .build()
            );
            allocator.deallocate(vm);
        });
    }


    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        final Session session = sessions.get(request.getSessionId());
        if (session == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        final var op = operations.create(
                "Allocating vm",
                session.owner(),
                Any.pack(AllocateMetadata.newBuilder().build())
        );

        final var existingVm = dao.acquire(request.getSessionId(), request.getPoolId());
        if (existingVm != null) {
            operations.update(op.complete(Any.pack(AllocateResponse.newBuilder()
                .setSessionId(existingVm.sessionId())
                .setPoolId(existingVm.poolId())
                .setVmId(existingVm.poolId())
                .putAllMetadata(existingVm.vmMeta())
                .build())));
            responseObserver.onNext(op.toGrpc());
            responseObserver.onCompleted();
            return;
        }
        responseObserver.onNext(op.toGrpc());
        responseObserver.onCompleted();
        var workloads = request.getWorkloadList().stream()
            .map(w -> new Workload(w.getName(), w.getImage(), w.getEnvMap(), w.getArgsList(), w.getPortBindingsMap()))
            .toList();
        var vm = dao.create(request.getSessionId(), request.getPoolId(), workloads);
        operations.update(
            op.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                .setVmId(vm.vmId())
                .build())));
        var meta = allocator.allocate(vm);
        dao.update(new Vm.VmBuilder(vm)
            .setAllocatorMeta(meta)
            .setState(Vm.State.CONNECTING)
            .setAllocationTimeoutAt(Instant.now().plus(config.allocationTimeout()))  // TODO(artolord) add to config
            .build());
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        var vm = dao.get(request.getVmId());
        if (vm == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot found vm").asException());
            return;
        }
        if (vm.state() != Vm.State.RUNNING) {
            LOG.error("Freed vm {} in status {}", vm, vm.state());
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }
        var session = sessions.get(vm.sessionId());
        if (session == null) {
            LOG.error("Corrupted vm with incorrect session id");
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }
        responseObserver.onNext(FreeResponse.newBuilder().build());
        responseObserver.onCompleted();

        dao.update(new Vm.VmBuilder(vm)
            .setState(Vm.State.IDLING)
            .setExpireAt(Instant.now().plus(session.minIdleTimeout()))
            .build());
    }
}
