package ai.lzy.allocator.services;


import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ProtoPrinter;
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
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import javax.inject.Named;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorPrivateService extends AllocatorPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorPrivateService.class);

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final VmAllocator allocator;
    private final SessionDao sessionsDao;
    private final Storage storage;
    private final ServiceConfig config;
    private final AllocatorMetrics metrics;

    public AllocatorPrivateService(VmDao vmDao, VmAllocator allocator, SessionDao sessionsDao,
                                   AllocatorDataSource storage, ServiceConfig config, AllocatorMetrics metrics,
                                   @Named("AllocatorOperationDao") OperationDao operationDao)
    {
        this.vmDao = vmDao;
        this.allocator = allocator;
        this.sessionsDao = sessionsDao;
        this.storage = storage;
        this.operationsDao = operationDao;
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        LOG.info("RegisterVM: {}", ProtoPrinter.safePrinter().shortDebugString(request));

        final Vm[] vmRef = {null};
        final Operation[] opRef = {null};

        try {
            var status = withRetries(
                LOG,
                () -> {
                    Vm vm;
                    try (final var transaction = TransactionHandle.create(storage)) {
                        vm = vmDao.get(request.getVmId(), transaction);
                        if (vm == null) {
                            metrics.registerFail.inc();
                            LOG.error("VM {} does not exist", request.getVmId());
                            return Status.NOT_FOUND.withDescription("Vm %s not found".formatted(request.getVmId()));
                        }
                        vmRef[0] = vm;

                        if (vm.status() == Vm.Status.RUNNING) {
                            metrics.registerFail.inc();
                            LOG.error("Vm {} has been already registered", vm);
                            return Status.ALREADY_EXISTS;
                        }

                        if (vm.status() != Vm.Status.ALLOCATING) {
                            metrics.registerFail.inc();
                            LOG.error("Wrong status of vm while register, expected ALLOCATING: {}", vm);
                            return Status.FAILED_PRECONDITION;
                        }

                        final var op = operationsDao.get(vm.allocOpId(), transaction);
                        if (op == null) {
                            metrics.registerFail.inc();
                            var opId = vm.allocOpId();
                            LOG.error("Operation {} does not exist", opId);
                            return Status.NOT_FOUND.withDescription("Op %s not found".formatted(opId));
                        }

                        if (op.done()) {
                            metrics.registerFail.inc();
                            var opId = vm.allocOpId();
                            LOG.error("Operation {} already done", opId);
                            return Status.CANCELLED.withDescription("Op %s already done".formatted(opId));
                        }

                        opRef[0] = op;

                        final var session = sessionsDao.get(vm.sessionId(), transaction);
                        if (session == null) {
                            metrics.registerFail.inc();
                            LOG.error("Session {} does not exist", vm.sessionId());
                            return Status.NOT_FOUND.withDescription("Session not found");
                        }

                        var activityDeadline = Instant.now().plus(config.getHeartbeatTimeout());
                        vmDao.setVmRunning(vm.vmId(), request.getMetadataMap(), activityDeadline, transaction);

                        final List<AllocateResponse.VmEndpoint> hosts;
                        try {
                            hosts = allocator.getVmEndpoints(vm.vmId(), transaction).stream()
                                .map(VmAllocator.VmEndpoint::toProto)
                                .toList();
                        } catch (Exception e) {
                            metrics.registerFail.inc();
                            LOG.error("Cannot get endpoints of vm {}", vm.vmId(), e);
                            return Status.INTERNAL.withDescription("Cannot get endpoints of vm");
                        }

                        var response = Any.pack(
                            AllocateResponse.newBuilder()
                                .setPoolId(vm.poolLabel())
                                .setSessionId(vm.sessionId())
                                .setVmId(vm.vmId())
                                .addAllEndpoints(hosts)
                                .putAllMetadata(request.getMetadataMap())
                                .build());
                        op.setResponse(response);

                        try {
                            operationsDao.complete(op.id(), response, transaction);
                        } catch (OperationCompletedException e) {
                            metrics.registerFail.inc();
                            LOG.error("Operation {} (VM {}) already completed", op.id(), vm.vmId());
                            return Status.CANCELLED.withDescription("Op %s already done".formatted(vm.vmId()));
                        }

                        transaction.commit();

                        metrics.registerSuccess.inc();
                        metrics.allocateNewDuration.observe(
                            Duration.between(vm.allocateState().startedAt(), Instant.now()).toSeconds());

                        metrics.runningVms.labels(vm.poolLabel()).inc();

                        return Status.OK;
                    }
                });

            if (status.isOk()) {
                LOG.info("Vm {} registered", request.getVmId());

                responseObserver.onNext(RegisterResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(status.asException());
            }
        } catch (Exception ex) {
            metrics.registerFail.inc();

            if (ex instanceof SQLException || vmRef[0] == null) {
                LOG.error("Error while registering vm {}: {}", request.getVmId(), ex.getMessage(), ex);
                responseObserver.onError(Status.UNAVAILABLE.withDescription(ex.getMessage()).asException());
                return;
            }

            LOG.error("Error while registering vm {}: {}", vmRef[0], ex.getMessage(), ex);

            var status = Status.INTERNAL
                .withDescription("Error while registering vm %s: %s".formatted(vmRef[0].vmId(), ex.getMessage()));
            responseObserver.onError(status.asException());

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        if (opRef[0] != null) {
                            operationsDao.fail(opRef[0].id(), toProto(status), tx);
                        }
                        vmDao.setStatus(vmRef[0].vmId(), Vm.Status.DELETING, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Cannot cleanup failed register: {}", vmRef[0], e);
            }
        }
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        Vm vm;
        try {
            vm = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> vmDao.get(request.getVmId(), null));
        } catch (Exception ex) {
            metrics.hbFail.inc();
            LOG.error("Cannot read VM {}: {}", request.getVmId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        if (vm == null) {
            metrics.hbUnknownVm.inc();
            LOG.error("Heartbeat from unknown VM {}", request.getVmId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Vm not found").asException());
            return;
        }


        if (!Set.of(Vm.Status.RUNNING, Vm.Status.IDLE).contains(vm.status())) {
            metrics.hbInvalidVm.inc();
            LOG.error("Wrong status of vm {} while receiving heartbeat: {}, expected RUNNING or IDLING",
                vm.vmId(), vm.status());
            responseObserver.onError(
                Status.FAILED_PRECONDITION.withDescription("Wrong state for heartbeat").asException());
            return;
        }

        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> vmDao.setLastActivityTime(vm.vmId(), Instant.now().plus(config.getHeartbeatTimeout()))
            );
        } catch (Exception ex) {
            metrics.hbFail.inc();
            LOG.error("Cannot update VM {} last activity time: {}", vm, ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(HeartbeatResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
