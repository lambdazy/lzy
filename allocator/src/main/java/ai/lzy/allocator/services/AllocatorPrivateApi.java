package ai.lzy.allocator.services;


import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.v1.AllocatorPrivateGrpc.AllocatorPrivateImplBase;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorPrivateApi.HeartbeatRequest;
import ai.lzy.v1.VmAllocatorPrivateApi.HeartbeatResponse;
import ai.lzy.v1.VmAllocatorPrivateApi.RegisterRequest;
import ai.lzy.v1.VmAllocatorPrivateApi.RegisterResponse;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Singleton
public class AllocatorPrivateApi extends AllocatorPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorPrivateApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;

    public AllocatorPrivateApi(VmDao dao, OperationDao operations, VmAllocator allocator) {
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        final var vm = dao.get(request.getVmId());
        if (vm == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
            return;
        }

        if (vm.state() != Vm.State.CONNECTING) {
            LOG.error("Wrong status of vm while register: {}", vm);
            responseObserver.onError(Status.INTERNAL.asException());
            allocator.deallocate(vm);
            dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build());
            return;
        }

        final var op = operations.get(vm.allocationOperationId());
        if (op == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Op not found").asException());
            allocator.deallocate(vm);
            dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build());
            return;
        }

        if (op.error() != null && op.error().getCode() == Status.Code.CANCELLED) {  // Op is cancelled by client
            allocator.deallocate(vm);
            dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build());
            return;
        }

        dao.update(new Vm.VmBuilder(vm)
            .setState(Vm.State.RUNNING)
            .setVmMeta(request.getMetadataMap())
            .setHeartBeatTimeoutAt(Instant.now().plus(10, ChronoUnit.MINUTES))  // TODO(artolord) add to config
            .build()
        );

        operations.update(op.complete(Any.pack(AllocateResponse.newBuilder()
            .setPoolId(vm.poolId())
            .setSessionId(vm.sessionId())
            .setVmId(vm.vmId())
            .putAllMetadata(request.getMetadataMap())
            .build())));
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        final var vm = dao.get(request.getVmId());
        if (vm == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
            return;
        }

        dao.update(new Vm.VmBuilder(vm)
            .setHeartBeatTimeoutAt(Instant.now().plus(10, ChronoUnit.MINUTES))  // TODO(artolord) add to config
            .build()
        );

    }
}
