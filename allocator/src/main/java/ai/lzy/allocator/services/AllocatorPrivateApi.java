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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

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
        final Vm[] vmRef = {null};
        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> {
                    Vm vm;
                    try (final var transaction = TransactionHandle.create(storage)) {
                        vm = dao.get(request.getVmId(), transaction);
                        if (vm == null) {
                            LOG.error("VM {} does not exist", request.getVmId());
                            metrics.unknownVm.inc();
                            try {
                                responseObserver.onError(
                                    Status.NOT_FOUND.withDescription("Vm with this id not found").asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        vmRef[0] = vm;

                        if (vm.status() == Vm.VmStatus.RUNNING) {
                            LOG.error("Vm {} has been already registered", vm);
                            metrics.alreadyRegistered.inc();
                            try {
                                responseObserver.onError(Status.ALREADY_EXISTS.asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        if (vm.status() == Vm.VmStatus.DEAD) {
                            LOG.error("Vm {} is DEAD", vm);
                            try {
                                responseObserver.onError(
                                    Status.INVALID_ARGUMENT.withDescription("VM is dead").asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        if (vm.status() != Vm.VmStatus.CONNECTING) {
                            LOG.error("Wrong status of vm while register, expected CONNECTING: {}", vm);
                            try {
                                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        final var op = operations.get(vm.allocationOperationId(), transaction);
                        if (op == null) {
                            LOG.error("Operation {} does not exist", vm.allocationOperationId());
                            try {
                                responseObserver.onError(
                                    Status.NOT_FOUND.withDescription("Op not found").asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        final var session = sessions.get(vm.sessionId(), transaction);
                        if (session == null) {
                            LOG.error("Session {} does not exist", vm.sessionId());
                            metrics.unknownSession.inc();
                            try {
                                responseObserver.onError(
                                    Status.NOT_FOUND.withDescription("Session not found").asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        if (op.error() != null && op.error().getCode() == Status.Code.CANCELLED) {
                            // Op is cancelled by client, add VM to cache
                            dao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()),
                                transaction);

                            transaction.commit();

                            metrics.registerCancelledVm.inc();
                            try {
                                responseObserver.onError(
                                    Status.NOT_FOUND.withDescription("Op not found").asException());
                            } catch (StatusRuntimeException e) {
                                LOG.error(e);
                            }
                            return;
                        }

                        dao.update(
                            vm.vmId(),
                            new Vm.VmStateBuilder(vm.state())
                                .setStatus(Vm.VmStatus.RUNNING)
                                .setVmMeta(request.getMetadataMap())
                                .setLastActivityTime(Instant.now().plus(config.getHeartbeatTimeout()))
                                .build(),
                            transaction);

                        final List<AllocateResponse.VmHost> hosts;
                    try {
                        hosts = allocator.vmHosts(vm.vmId(), transaction).stream()
                            .map(h -> AllocateResponse.VmHost.newBuilder()
                                .setType(h.type())
                                .setValue(h.value())
                                .build())
                            .toList();
                    } catch (Exception e) {
                        LOG.error("Cannot get hosts of vm {}", vm.vmId());
                        responseObserver.onError(Status.INTERNAL
                            .withDescription("Cannot get hosts of vm").asException());
                        return null;
                    }

                    op.setResponse(Any.pack(AllocateResponse.newBuilder()
                        .setPoolId(vm.poolLabel())
                        .setSessionId(vm.sessionId())
                        .setVmId(vm.vmId())
                        .addAllHosts(hosts)
                            .putAllMetadata(request.getMetadataMap())
                            .build()));
                        operations.update(op, transaction);
                        transaction.commit();

                        metrics.registered.inc();
                        metrics.allocationTime.observe(
                            Duration.between(vm.allocationStartedAt(), Instant.now()).toSeconds());
                    }
                });
        } catch (Exception ex) {
            var vm = vmRef[0];

            LOG.error("Error while registering vm {}: {}",
                vm != null ? vm.toString() : request.getVmId(), ex.getMessage(), ex);

            metrics.failed.inc();

            responseObserver.onError(
                Status.INTERNAL.withDescription("Error while registering vm %s: %s".formatted(vm, ex.getMessage()))
                    .asException());

            if (vm != null) {
                LOG.info("Deallocating failed vm {}", vm);
                allocator.deallocate(vm.vmId());
            }
            return;
        }

        responseObserver.onNext(RegisterResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        Vm vm;
        try {
            vm = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> dao.get(request.getVmId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot read VM {}: {}", request.getVmId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        if (vm == null) {
            LOG.error("Heartbeat from unknown VM {}", request.getVmId());
            metrics.hbUnknownVm.inc();
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Vm not found").asException());
            return;
        }


        if (!Set.of(Vm.VmStatus.RUNNING, Vm.VmStatus.IDLE).contains(vm.status())) {
            LOG.error("Wrong status of vm {} while receiving heartbeat: {}, expected RUNNING or IDLING",
                vm.vmId(), vm.status());
            metrics.hbInvalidVm.inc();
            responseObserver.onError(
                Status.FAILED_PRECONDITION.withDescription("Wrong state for heartbeat").asException());
        }

        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> dao.updateLastActivityTime(vm.vmId(), Instant.now().plus(config.getHeartbeatTimeout()))
            );
        } catch (Exception ex) {
            LOG.error("Cannot update VM {} last activity time: {}", vm, ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(HeartbeatResponse.getDefaultInstance());
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

        private final Histogram allocationTime = Histogram
            .build("allocation_time", "Total allocation time (sec), from request till register")
            .subsystem("allocator")
            .buckets(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 45, 60)
            .register();
    }
}
