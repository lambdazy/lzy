package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskStorage;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.volume.DiskVolumeDescription;
import ai.lzy.allocator.volume.HostPathVolumeDescription;
import ai.lzy.allocator.volume.VolumeRequest;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.VmAllocatorApi.*;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorApi extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorApi.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final DiskStorage diskStorage;
    private final SessionDao sessions;
    private final VmAllocator allocator;
    private final ServiceConfig config;
    private final AllocatorDataSource storage;
    private final Metrics metrics = new Metrics();
    private final SubjectServiceGrpcClient subjectClient;

    @Inject
    public AllocatorApi(VmDao dao, OperationDao operations, SessionDao sessions, DiskStorage diskStorage,
                        VmAllocator allocator, ServiceConfig config, AllocatorDataSource storage,
                        @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel)
    {
        this.dao = dao;
        this.operations = operations;
        this.sessions = sessions;
        this.diskStorage = diskStorage;
        this.allocator = allocator;
        this.config = config;
        this.storage = storage;

        this.subjectClient = new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
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

        Session session;
        try {
            session = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> sessions.create(request.getOwner(), policy, null));
        } catch (Exception ex) {
            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(CreateSessionResponse.newBuilder()
            .setSessionId(session.sessionId())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> {
                    sessions.delete(request.getSessionId(), null);
                    dao.delete(request.getSessionId());
                });
        } catch (Exception ex) {
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        LOG.info("Allocation request {}", JsonUtils.printSingleLine(request));

        final var startedAt = Instant.now();

        Session session;
        try {
            session = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> sessions.get(request.getSessionId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get session {}: {}", request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        ai.lzy.allocator.model.Operation op;
        try {
            op = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.create("Allocating VM", session.owner(),
                    Any.pack(AllocateMetadata.getDefaultInstance()), null)
            );
        } catch (Exception ex) {
            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        BiConsumer<ai.lzy.allocator.model.Operation, String> failOperation = (op1, msg) -> {
            op1.setError(Status.INTERNAL.withDescription(msg));

            try {
                withRetries(
                    defaultRetryPolicy(),
                    LOG,
                    () -> {
                        operations.update(op1, null);
                        return null;
                    });
            } catch (Exception ex) {
                LOG.error("Cannot fail operation {} with reason {}: {}", op1, msg, ex.getMessage(), ex);
            }
        };

        Vm.Spec spec;
        try {
            spec = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> {
                    try (var transaction = TransactionHandle.create(storage)) {
                        final var existingVm = dao.acquire(request.getSessionId(), request.getPoolLabel(),
                            request.getZone(), transaction);

                        if (existingVm != null) {
                            LOG.info("Found existing VM {}", existingVm);

                            op.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                                .setVmId(existingVm.vmId())
                                .build()));
                            op.setResponse(Any.pack(AllocateResponse.newBuilder()
                                .setSessionId(existingVm.sessionId())
                                .setPoolId(existingVm.poolLabel())
                                .setVmId(existingVm.vmId())
                                .putAllMetadata(existingVm.vmMeta())
                                .build()));
                            operations.update(op, transaction);

                            transaction.commit();

                            metrics.allocateVmExisting.inc();
                            return null;
                        }

                        var workloads = request.getWorkloadList().stream()
                            .map(Workload::fromProto)
                            .toList();
                        final List<VolumeRequest> volumes = getVolumeRequests(request, op, transaction);
                        if (volumes == null) {
                            return null;
                        }

                        var vmSpec = dao.create(request.getSessionId(), request.getPoolLabel(), request.getZone(),
                            workloads, volumes, op.id(), startedAt, transaction);

                        op.modifyMeta(Any.pack(AllocateMetadata.newBuilder()
                            .setVmId(vmSpec.vmId())
                            .build()));

                        operations.update(op, transaction);

                        final var vmState = new Vm.VmStateBuilder()
                            .setStatus(Vm.VmStatus.CONNECTING)
                            .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                            .build();
                        dao.update(vmSpec.vmId(), vmState, transaction);

                        transaction.commit();
                        metrics.allocateVmNew.inc();

                        return vmSpec;
                    }
                });
        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            failOperation.accept(op, ex.getMessage());

            metrics.allocationError.inc();

            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();

        {
            var vmSubj = subjectClient.createSubject(AuthProvider.INTERNAL, "vm-" + spec.vmId(), SubjectType.SERVANT);
            var token = UUID.randomUUID().toString();
            subjectClient.addCredentials(vmSubj, "main", token, CredentialsType.OTT);
        }

        try {
            try {
                var timer = metrics.allocateDuration.startTimer();
                allocator.allocate(spec);
                timer.close();
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating: {}", e.getMessage(), e);
                metrics.allocationError.inc();
                op.setError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                operations.update(op, null);
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            failOperation.accept(op, "Error while executing request");
        }
    }

    @Nullable
    private List<VolumeRequest> getVolumeRequests(
        AllocateRequest request, ai.lzy.allocator.model.Operation op, TransactionHandle transaction
    ) throws SQLException
    {
        final List<VolumeRequest> volumes = new ArrayList<>();
        for (var volume: request.getVolumesList()) {
            if (volume.hasHostPathVolume()) {
                final var hostPathVolume = volume.getHostPathVolume();
                volumes.add(new VolumeRequest(new HostPathVolumeDescription(
                    volume.getName(),
                    hostPathVolume.getPath(),
                    HostPathVolumeDescription.HostPathType.valueOf(
                        hostPathVolume.getHostPathType().name()))
                ));
            } else if (volume.hasDiskVolume()) {
                final var diskVolume = volume.getDiskVolume();
                final Disk disk = diskStorage.get(diskVolume.getDiskId(), transaction);
                if (disk == null) {
                    final String message =
                        "Disk with id %s not found in diskStorage".formatted(diskVolume.getDiskId());
                    LOG.error(message);
                    op.setError(Status.NOT_FOUND.withDescription(message));
                    operations.update(op, transaction);
                    transaction.commit();
                    return null;
                }

                volumes.add(new VolumeRequest(new DiskVolumeDescription(
                    volume.getName(), diskVolume.getDiskId(), disk.spec().sizeGb()
                )));
            } else {
                final String message = "Unknown volumeType %s for volume=%s"
                    .formatted(volume.getVolumeTypeCase(), volume.getName());
                LOG.error(message);
                op.setError(Status.INVALID_ARGUMENT.withDescription(message));
                operations.update(op, transaction);
                transaction.commit();
                return null;
            }
        }
        return volumes;
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        Status status;
        try {
            status = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> {
                    try (var transaction = TransactionHandle.create(storage)) {
                        var vm = dao.get(request.getVmId(), transaction);
                        if (vm == null) {
                            LOG.error("Cannot find vm {}", request.getVmId());
                            return Status.NOT_FOUND.withDescription("Cannot find vm");
                        }

                        // TODO(artolord) validate that client can free this vm
                        if (vm.status() != Vm.VmStatus.RUNNING) {
                            LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
                            return Status.FAILED_PRECONDITION.withDescription("State is " + vm.state());
                        }

                        var session = sessions.get(vm.sessionId(), transaction);
                        if (session == null) {
                            LOG.error("Corrupted vm with incorrect session id: {}", vm);
                            return Status.INTERNAL;
                        }

                        dao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()), transaction);

                        transaction.commit();
                        return Status.OK;
                    }
                });
        } catch (Exception ex) {
            LOG.error("Error while free vm {}: {}", request.getVmId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while free").asException());
            return;
        }

        if (Status.Code.OK.equals(status.getCode())) {
            responseObserver.onNext(FreeResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(status.asException());
        }
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
