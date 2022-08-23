package ai.lzy.allocator.services;


import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
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

import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;

@Singleton
public class AllocatorPrivateApi extends AllocatorPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorPrivateApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final SessionDao sessions;
    private final Storage storage;
    private final ServiceConfig config;

    public AllocatorPrivateApi(VmDao dao,
                               OperationDao operations,
                               VmAllocator allocator,
                               SessionDao sessions,
                               Storage storage,
                               ServiceConfig config) {
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
        this.sessions = sessions;
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        Vm vm = null;
        try (final var transaction = new TransactionHandle(storage)) {
            vm = dao.get(request.getVmId(), transaction);
            if (vm == null) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
                return;
            }

            if (vm.state() == Vm.State.RUNNING) {
                LOG.error("Vm {} has been already registered", vm);
                responseObserver.onError(Status.ALREADY_EXISTS.asException());
                return;
            } else if (vm.state() != Vm.State.CONNECTING) {
                LOG.error("Wrong status of vm while register, expected CONNECTING: {}", vm);
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            final var op = operations.get(vm.allocationOperationId(), transaction);
            if (op == null) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Op not found").asException());
                return;
            }

            final var session = sessions.get(vm.sessionId(), transaction);
            if (session == null) {
                LOG.error("Session {} does not exist", vm.sessionId());
                responseObserver.onError(Status.NOT_FOUND.withDescription("Session not found").asException());
                return;
            }

            if (op.error() != null && op.error().getCode() == Status.Code.CANCELLED) {
                // Op is cancelled by client, add VM to cache
                dao.update(new Vm.VmBuilder(vm).setDeadline(Instant.now().plus(session.cachePolicy().minIdleTimeout()))
                    .setState(Vm.State.IDLE).build(), transaction);
                transaction.commit();
                responseObserver.onError(Status.NOT_FOUND.withDescription("Op not found").asException());
                return;
            }
            dao.update(new Vm.VmBuilder(vm)
                .setState(Vm.State.RUNNING)
                .setVmMeta(request.getMetadataMap())
                .setLastActivityTime(Instant.now().plus(config.getHeartbeatTimeout()))  // TODO(artolord) add to config
                .build(), transaction);

            operations.update(op.complete(Any.pack(AllocateResponse.newBuilder()
                .setPoolId(vm.poolLabel())
                .setSessionId(vm.sessionId())
                .setVmId(vm.vmId())
                .putAllMetadata(request.getMetadataMap())
                .build())), transaction);

            transaction.commit();
        } catch (SQLException e) {
            LOG.error("Error while registering vm", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while registering vm").asException());
            if (vm != null) {
                allocator.deallocate(vm);
            }
            return;
        }
        responseObserver.onNext(RegisterResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        final var vm = dao.get(request.getVmId(), null);
        if (vm == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
            return;
        }

        if (!Set.of(Vm.State.RUNNING, Vm.State.IDLE).contains(vm.state())) {
            LOG.error("Wrong status of vm while receiving heartbeat: {}, expected RUNNING or IDLING", vm.state());
            responseObserver.onError(
                Status.FAILED_PRECONDITION.withDescription("Wrong state for heartbeat").asException());
        }

        dao.update(new Vm.VmBuilder(vm)
            .setLastActivityTime(Instant.now().plus(config.getHeartbeatTimeout()))  // TODO(artolord) add to config
            .build(), null);

        responseObserver.onNext(HeartbeatResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
