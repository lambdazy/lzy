package ai.lzy.allocator.services;


import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.model.Vm;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorPrivateGrpc.AllocatorPrivateImplBase;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorPrivateApi.HeartbeatRequest;
import ai.lzy.v1.VmAllocatorPrivateApi.HeartbeatResponse;
import ai.lzy.v1.VmAllocatorPrivateApi.RegisterRequest;
import ai.lzy.v1.VmAllocatorPrivateApi.RegisterResponse;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import javax.inject.Named;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorPrivateService extends AllocatorPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorPrivateService.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final SessionDao sessions;
    private final Storage storage;
    private final ServiceConfig config;
    private final Metrics metrics = new Metrics();
    private final SubjectServiceClient subjectClient;

    public AllocatorPrivateService(VmDao dao, VmAllocator allocator, SessionDao sessions,
                                   AllocatorDataSource storage, ServiceConfig config,
                                   @Named("AllocatorOperationDao") OperationDao operationDao,
                                   @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                                   @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.dao = dao;
        this.allocator = allocator;
        this.sessions = sessions;
        this.storage = storage;
        this.operations = operationDao;
        this.config = config;
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        final Vm[] vmRef = {null};
        try {
            var status = withRetries(
                LOG,
                () -> {
                    Vm vm;
                    try (final var transaction = TransactionHandle.create(storage)) {
                        vm = dao.get(request.getVmId(), transaction);
                        if (vm == null) {
                            LOG.error("VM {} does not exist", request.getVmId());
                            metrics.unknownVm.inc();
                            return Status.NOT_FOUND.withDescription("Vm with this id not found");
                        }

                        vmRef[0] = vm;

                        if (vm.status() == Vm.Status.RUNNING) {
                            LOG.error("Vm {} has been already registered", vm);
                            metrics.alreadyRegistered.inc();
                            return Status.ALREADY_EXISTS;
                        }

                        if (vm.status() == Vm.Status.DEAD) {
                            LOG.error("Vm {} is DEAD", vm);
                            return Status.INVALID_ARGUMENT.withDescription("VM is dead");
                        }

                        if (vm.status() != Vm.Status.ALLOCATING) {
                            LOG.error("Wrong status of vm while register, expected ALLOCATING: {}", vm);
                            return Status.FAILED_PRECONDITION;
                        }

                        final var op = operations.get(vm.allocOpId(), transaction);
                        if (op == null) {
                            var opId = vm.allocOpId();
                            LOG.error("Operation {} does not exist", opId);
                            return Status.NOT_FOUND.withDescription("Op %s not found".formatted(opId));
                        }

                        final var session = sessions.get(vm.sessionId(), transaction);
                        if (session == null) {
                            LOG.error("Session {} does not exist", vm.sessionId());
                            metrics.unknownSession.inc();
                            return Status.NOT_FOUND.withDescription("Session not found");
                        }

                        if (op.error() != null && op.error().getCode() == Status.Code.CANCELLED) {
                            // Op is cancelled by client, add VM to cache
                            dao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()),
                                transaction);

                            transaction.commit();

                            metrics.registerCancelledVm.inc();
                            return Status.NOT_FOUND.withDescription("Op not found");
                        }

                        var activityDeadline = Instant.now().plus(config.getHeartbeatTimeout());
                        dao.setVmRunning(vm.vmId(), request.getMetadataMap(), activityDeadline, transaction);

                        final List<AllocateResponse.VmEndpoint> hosts;
                        try {
                            hosts = allocator.getVmEndpoints(vm.vmId(), transaction).stream()
                                .map(VmAllocator.VmEndpoint::toProto)
                                .toList();
                        } catch (Exception e) {
                            LOG.error("Cannot get endpoints of vm {}", vm.vmId(), e);
                            return Status.INTERNAL.withDescription("Cannot get endpoints of vm");
                        }

                        var response = Any.pack(AllocateResponse.newBuilder()
                            .setPoolId(vm.poolLabel())
                            .setSessionId(vm.sessionId())
                            .setVmId(vm.vmId())
                            .addAllEndpoints(hosts)
                            .putAllMetadata(request.getMetadataMap())
                            .build());
                        op.setResponse(response);
                        operations.updateResponse(op.id(), response.toByteArray(), transaction);

                        transaction.commit();

                        metrics.registered.inc();
                        metrics.allocationTime.observe(
                            Duration.between(vm.allocateState().startedAt(), Instant.now()).toSeconds());

                        return Status.OK;
                    }
                });

            if (status.isOk()) {
                responseObserver.onNext(RegisterResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(status.asException());
            }
        } catch (Exception ex) {
            var vm = vmRef[0];

            LOG.error("Error while registering vm {}: {}",
                vm != null ? vm.toString() : request.getVmId(), ex.getMessage(), ex);

            metrics.failed.inc();

            responseObserver.onError(Status.INTERNAL
                .withDescription("Error while registering vm %s: %s".formatted(vm, ex.getMessage())).asException());

            // TODO: do it another thread, don't occupy grpc thread
            if (vm != null) {
                LOG.info("Deallocating failed vm {}", vm);

                var vmSubjId = vm.allocateState().vmSubjectId();
                if (vmSubjId != null) {
                    try {
                        subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjId));
                    } catch (Exception e) {
                        LOG.error("Cannot remove VM subject {}: {}", vmSubjId, e.getMessage(), e);
                    }
                }

                allocator.deallocate(vm.vmId());
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


        if (!Set.of(Vm.Status.RUNNING, Vm.Status.IDLE).contains(vm.status())) {
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
                () -> dao.setLastActivityTime(vm.vmId(), Instant.now().plus(config.getHeartbeatTimeout()))
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
