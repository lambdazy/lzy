package ai.lzy.allocator.services;


import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.Vm;
import ai.lzy.metrics.MetricReporter;
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
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Set;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorPrivateApi extends AllocatorPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorPrivateApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final SessionDao sessions;
    private final Storage storage;
    private final ServiceConfig config;
    private final Metrics metrics = new Metrics();

    @Inject
    public AllocatorPrivateApi(VmDao dao, OperationDao operations, VmAllocator allocator, SessionDao sessions,
                               AllocatorDataSource storage, ServiceConfig config)
    {
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
                LOG.error("VM {} does not exist", request.getVmId());
                metrics.unknownVm.inc();
                responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
                return;
            }

            if (vm.state() == Vm.State.RUNNING) {
                LOG.error("Vm {} has been already registered", vm);
                metrics.alreadyRegistered.inc();
                responseObserver.onError(Status.ALREADY_EXISTS.asException());
                return;
            }

            if (vm.state() == Vm.State.DEAD) {
                LOG.error("Vm {} is DEAD", vm);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("VM is dead").asException());
                return;
            }

            if (vm.state() != Vm.State.CONNECTING) {
                LOG.error("Wrong status of vm while register, expected CONNECTING: {}", vm);
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            final var op = operations.get(vm.allocationOperationId(), transaction);
            if (op == null) {
                LOG.error("Operation {} does not exist", vm.allocationOperationId());
                responseObserver.onError(Status.NOT_FOUND.withDescription("Op not found").asException());
                return;
            }

            final var session = sessions.get(vm.sessionId(), transaction);
            if (session == null) {
                LOG.error("Session {} does not exist", vm.sessionId());
                metrics.unknownSession.inc();
                responseObserver.onError(Status.NOT_FOUND.withDescription("Session not found").asException());
                return;
            }

            if (op.error() != null && op.error().getCode() == Status.Code.CANCELLED) {
                // Op is cancelled by client, add VM to cache
                dao.update(
                    Vm.from(vm)
                        .setDeadline(Instant.now().plus(session.cachePolicy().minIdleTimeout()))
                        .setState(Vm.State.IDLE)
                        .build(),
                    transaction);

                transaction.commit();

                metrics.registerCancelledVm.inc();
                responseObserver.onError(Status.NOT_FOUND.withDescription("Op not found").asException());
                return;
            }

            dao.update(
                Vm.from(vm)
                    .setState(Vm.State.RUNNING)
                    .setVmMeta(request.getMetadataMap())
                    .setLastActivityTime(Instant.now().plus(config.getHeartbeatTimeout()))
                    .build(),
                transaction);

            operations.update(
                op.complete(Any.pack(AllocateResponse.newBuilder()
                    .setPoolId(vm.poolLabel())
                    .setSessionId(vm.sessionId())
                    .setVmId(vm.vmId())
                    .putAllMetadata(request.getMetadataMap())
                    .build())),
                transaction);

            transaction.commit();

            metrics.registered.inc();
        } catch (Exception e) {
            LOG.error("Error while registering vm {}: {}",
                vm != null ? vm.toString() : request.getVmId(), e.getMessage(), e);

            metrics.failed.inc();

            responseObserver.onError(
                Status.INTERNAL.withDescription("Error while registering vm %s: %s".formatted(vm, e.getMessage()))
                    .asException());

            if (vm != null) {
                LOG.info("Deallocating failed vm {}", vm);
                allocator.deallocate(vm);
            }
            return;
        }

        responseObserver.onNext(RegisterResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        final var vm = dao.get(request.getVmId(), null);
        if (vm == null) {
            LOG.error("Heartbeat from unknown VM {}", request.getVmId());
            metrics.hbUnknownVm.inc();
            responseObserver.onError(Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
            return;
        }

        if (!Set.of(Vm.State.RUNNING, Vm.State.IDLE).contains(vm.state())) {
            LOG.error("Wrong status of vm {} while receiving heartbeat: {}, expected RUNNING or IDLING",
                vm.vmId(), vm.state());
            metrics.hbInvalidVm.inc();
            responseObserver.onError(
                Status.FAILED_PRECONDITION.withDescription("Wrong state for heartbeat").asException());
        }

        dao.update(
            Vm.from(vm)
                .setLastActivityTime(Instant.now().plus(config.getHeartbeatTimeout()))
                .build(),
            null);

        responseObserver.onNext(HeartbeatResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static final class Metrics {
        private final Counter registered = Counter
            .build("registered", "Successfully registered VMs")
            .subsystem("allocator_private")
            .register();

        private final Counter failed = Counter
            .build("failed", "Exceptionally failed registrations")
            .subsystem("allocator_private")
            .register();

        private final Counter unknownVm = Counter
            .build("unknown_vm", "Request from unknown VM")
            .subsystem("allocator_private")
            .register();

        private final Counter unknownSession = Counter
            .build("unknown_session", "Request to unknown session")
            .subsystem("allocator_private")
            .register();

        private final Counter alreadyRegistered = Counter
            .build("already_registered", "VM already registered")
            .subsystem("allocator_private")
            .register();

        private final Counter registerCancelledVm = Counter
            .build("register_cancelled_vm", "Registered VM already cancelled by client")
            .subsystem("allocator_private")
            .register();

        private final Counter hbUnknownVm = Counter
            .build("hb_unknown_vm", "Heartbits from unknown VMs")
            .subsystem("allocator_private")
            .register();

        private final Counter hbInvalidVm = Counter
            .build("hb_invalid_vm", "Heartbits from VMs in invalid states")
            .subsystem("allocator_private")
            .register();
    }
}
