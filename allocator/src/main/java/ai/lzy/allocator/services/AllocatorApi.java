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
import org.glassfish.jersey.internal.util.Producer;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

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

        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> sessions.create(request.getOwner(), policy, null),
            session -> {
                responseObserver.onNext(CreateSessionResponse.newBuilder()
                    .setSessionId(session.sessionId())
                    .build());
                responseObserver.onCompleted();
            },
            ex -> {
                LOG.error("Cannot create session: {}", ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription(ex.getMessage()).asException());
            });
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                sessions.delete(request.getSessionId(), null);
                dao.delete(request.getSessionId());
                return (Void) null;
            },
            ok -> {
                responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
                responseObserver.onCompleted();
            },
            ex -> {
                LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                    request.getSessionId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription(ex.getMessage()).asException());
            });
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        LOG.info("Allocation request {}", JsonUtils.printSingleLine(request));

        final var startedAt = Instant.now();

        final Session[] session = {null};
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> sessions.get(request.getSessionId(), null),
            ss -> {
                if (ss != null) {
                    session[0] = ss;
                } else {
                    responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
                }
            },
            ex -> {
                LOG.error("Cannot get session {}: {}", request.getSessionId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        );

        if (session[0] == null) {
            return;
        }

        final ai.lzy.allocator.model.Operation[] opRef = {null};
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> operations.create(UUID.randomUUID().toString(), "Allocating VM", session[0].owner(),
                Any.pack(AllocateMetadata.getDefaultInstance()), null),
            op -> opRef[0] = op,
            ex -> {
                LOG.error("Cannot create allocate vm operation for session {}: {}",
                    request.getSessionId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        );

        if (opRef[0] == null) {
            return;
        }

        Consumer<String> failOperation = msg -> {
            opRef[0] = opRef[0].complete(Status.INTERNAL.withDescription(msg));

            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> {
                    operations.update(opRef[0], null);
                    return null;
                },
                ok -> {},
                ex -> LOG.error("Cannot fail operation {} with reason {}: {}", opRef[0], msg, ex.getMessage(), ex));
        };

        final Vm[] vmRef = {null};
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    final var existingVm = dao.acquire(request.getSessionId(), request.getPoolLabel(),
                        request.getZone(), transaction);

                    if (existingVm != null) {
                        LOG.info("Found existing VM {}", existingVm);

                        opRef[0] = opRef[0].modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                            .setVmId(existingVm.vmId())
                            .build()));
                        opRef[0] = opRef[0].complete(Any.pack(AllocateResponse.newBuilder()
                            .setSessionId(existingVm.sessionId())
                            .setPoolId(existingVm.poolLabel())
                            .setVmId(existingVm.vmId())
                            .putAllMetadata(existingVm.vmMeta())
                            .build()));
                        operations.update(opRef[0], transaction);

                        transaction.commit();

                        metrics.allocateVmExisting.inc();
                        return null;
                    }

                    var workloads = request.getWorkloadList().stream()
                        .map(Workload::fromProto)
                        .toList();

                    var vm = dao.create(request.getSessionId(), request.getPoolLabel(), request.getZone(), workloads,
                        opRef[0].id(), startedAt, transaction);

                    opRef[0] = opRef[0].modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                        .setVmId(vm.vmId())
                        .build()));

                    operations.update(opRef[0], transaction);

                    vm = Vm.from(vm)
                        .setState(Vm.State.CONNECTING)
                        .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                        .build();
                    dao.update(vm, transaction);

                    transaction.commit();
                    metrics.allocateVmNew.inc();

                    return vm;
                }
            },
            vm -> {
                vmRef[0] = vm;
                responseObserver.onNext(opRef[0].toProto());
                responseObserver.onCompleted();
            },
            ex -> {
                LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
                failOperation.accept(ex.getMessage());

                metrics.allocationError.inc();

                responseObserver.onNext(opRef[0].toProto());
                responseObserver.onCompleted();
            }
        );

        if (vmRef[0] == null) {
            return;
        }

        try {
            try {
                var timer = metrics.allocateDuration.startTimer();
                allocator.allocate(vmRef[0]);
                timer.close();
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating: {}", e.getMessage(), e);
                metrics.allocationError.inc();
                operations.update(opRef[0].complete(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            failOperation.accept("Error while executing request");
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    var vm = dao.get(request.getVmId(), transaction);
                    if (vm == null) {
                        return (Producer<Status>) () -> Status.NOT_FOUND.withDescription("Cannot find vm");
                    }

                    // TODO(artolord) validate that client can free this vm
                    if (vm.state() != Vm.State.RUNNING) {
                        return (Producer<Status>) () -> {
                            LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
                            return Status.FAILED_PRECONDITION.withDescription("State is " + vm.state());
                        };
                    }

                    var session = sessions.get(vm.sessionId(), transaction);
                    if (session == null) {
                        return (Producer<Status>) () -> {
                            LOG.error("Corrupted vm with incorrect session id: {}", vm);
                            return Status.INTERNAL;
                        };
                    }

                    dao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()), transaction);

                    transaction.commit();
                    return (Producer<Status>) () -> Status.OK;
                }
            },
            st -> {
                var result = st.call();
                if (Status.Code.OK.equals(result.getCode())) {
                    responseObserver.onNext(FreeResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(result.asException());
                }
            },
            ex -> {
                LOG.error("Error while free vm {}: {}", request.getVmId(), ex.getMessage(), ex);
                responseObserver.onError(Status.INTERNAL.withDescription("Error while free").asException());
            }
        );
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
