package ai.lzy.allocator.services;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
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
import ai.lzy.allocator.volume.NFSVolumeDescription;
import ai.lzy.allocator.volume.VolumeRequest;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi.*;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.validation.LV;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.inject.Named;

import static ai.lzy.longrunning.RequestHash.md5;
import static ai.lzy.longrunning.dao.OperationDao.OPERATION_IDEMPOTENCY_KEY_CONSTRAINT;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.isUniqueViolation;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(beans = MetricReporter.class)
public class AllocatorApi extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorApi.class);

    private static final ProtoPrinter.Printer SAFE_PRINTER =
        ProtoPrinter.printer().usingSensitiveExtension(LV.sensitive);

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final DiskStorage diskStorage;
    private final SessionDao sessionsDao;
    private final VmAllocator allocator;
    private final TunnelAllocator tunnelAllocator;
    private final ServiceConfig config;
    private final AllocatorDataSource storage;
    private final Metrics metrics = new Metrics();
    private final SubjectServiceGrpcClient subjectClient;

    @Inject
    public AllocatorApi(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao, SessionDao sessionsDao,
                        DiskStorage diskStorage, VmAllocator allocator, TunnelAllocator tunnelAllocator,
                        ServiceConfig config, AllocatorDataSource storage,
                        @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                        @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.vmDao = vmDao;
        this.operationsDao = operationsDao;
        this.sessionsDao = sessionsDao;
        this.diskStorage = diskStorage;
        this.allocator = allocator;
        this.tunnelAllocator = tunnelAllocator;
        this.config = config;
        this.storage = storage;

        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);
    }

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        var idempotencyToken = GrpcHeaders.getIdempotencyKey();
        ai.lzy.longrunning.Operation.IdempotencyKey idempotencyKey = null;

        if (idempotencyToken != null) {
            idempotencyKey = new ai.lzy.longrunning.Operation.IdempotencyKey(idempotencyToken, md5(request));
            if (loadExistingOp(idempotencyKey, responseObserver)) {
                return;
            }
        }

        final var operationId = UUID.randomUUID().toString();
        final var sessionId = UUID.randomUUID().toString();

        final var response = CreateSessionResponse.newBuilder()
            .setSessionId(sessionId)
            .build();

        final var op = ai.lzy.longrunning.Operation.createCompleted(
            operationId,
            request.getOwner(),
            "CreateSession: " + request.getDescription(),
            idempotencyKey,
            /* meta */ null,
            Any.pack(response));

        final var minIdleTimeout = ProtoConverter.fromProto(request.getCachePolicy().getIdleTimeout());
        final var session = new Session(sessionId, request.getOwner(), request.getDescription(),
            new CachePolicy(minIdleTimeout), operationId);

        try {
            withRetries(
                LOG,
                () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        operationsDao.create(op, tx);
                        sessionsDao.create(session, tx);
                        tx.commit();
                    }
                });
        } catch (Exception ex) {
            if (idempotencyKey != null && isUniqueViolation(ex, OPERATION_IDEMPOTENCY_KEY_CONSTRAINT)) {
                if (loadExistingOp(idempotencyKey, responseObserver)) {
                    return;
                }

                LOG.error("Idempotency key {} not found", idempotencyToken);
                responseObserver.onError(Status.INTERNAL.withDescription("Idempotency key conflict").asException());
                return;
            }

            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        try {
            withRetries(
                LOG,
                () -> {
                    sessionsDao.delete(request.getSessionId(), null);
                    vmDao.delete(request.getSessionId());
                });
        } catch (Exception ex) {
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        LOG.info("Allocation request {}", SAFE_PRINTER.shortDebugString(request));
        final var startedAt = Instant.now();

        final var idempotencyToken = GrpcHeaders.getIdempotencyKey();
        ai.lzy.longrunning.Operation.IdempotencyKey idempotencyKey = null;

        if (idempotencyToken != null) {
            idempotencyKey = new ai.lzy.longrunning.Operation.IdempotencyKey(idempotencyToken, md5(request));
            if (loadExistingOp(idempotencyKey, responseObserver)) {
                return;
            }
        }

        final Inet6Address proxyV6Address;
        if (request.hasProxyV6Address()) {
            try {
                proxyV6Address = (Inet6Address) Inet6Address.getByName(request.getProxyV6Address());
            } catch (UnknownHostException e) {
                LOG.error("Invalid proxy v6 address {} in allocate request", request.getProxyV6Address());
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
                return;
            }
        } else {
            proxyV6Address = null;
        }

        final Session session;
        try {
            session = withRetries(LOG, () -> sessionsDao.get(request.getSessionId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get session {}: {}", request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            LOG.error("Cannot allocate, session not found. Request: {}", SAFE_PRINTER.shortDebugString(request));
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        final var op = new ai.lzy.longrunning.Operation(
            UUID.randomUUID().toString(),
            session.owner(),
            Instant.now(),
            "AllocateVM: pool=%s, zone=%s".formatted(request.getPoolLabel(), request.getZone()),
            idempotencyKey,
            Any.pack(AllocateMetadata.getDefaultInstance()),
            Instant.now(),
            /* done */ false,
            null,
            null);

        try {
            withRetries(LOG, () -> operationsDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null && isUniqueViolation(ex, OPERATION_IDEMPOTENCY_KEY_CONSTRAINT)) {
                if (loadExistingOp(idempotencyKey, responseObserver)) {
                    return;
                }

                LOG.error("Idempotency key {} not found", idempotencyToken);
                responseObserver.onError(Status.INTERNAL.withDescription("Idempotency key conflict").asException());
                return;
            }

            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        var vmOtt = UUID.randomUUID().toString();

        Vm.Spec spec;
        try {
            spec = withRetries(
                LOG,
                () -> {
                    try (var transaction = TransactionHandle.create(storage)) {
                        final var existingVm = vmDao.acquire(request.getSessionId(), request.getPoolLabel(),
                            request.getZone(), transaction);

                        if (existingVm != null) {
                            LOG.info("Found existing VM {}", existingVm);

                            var meta = Any.pack(AllocateMetadata.newBuilder()
                                .setVmId(existingVm.vmId())
                                .build());
                            var response = Any.pack(AllocateResponse.newBuilder()
                                .setSessionId(existingVm.sessionId())
                                .setPoolId(existingVm.poolLabel())
                                .setVmId(existingVm.vmId())
                                .putAllMetadata(existingVm.vmMeta())
                                .build());

                            op.modifyMeta(meta);
                            op.setResponse(response);

                            operationsDao.updateMetaAndResponse(op.id(), meta.toByteArray(), response.toByteArray(),
                                transaction);

                            transaction.commit();
                            responseObserver.onNext(op.toProto());
                            responseObserver.onCompleted();

                            metrics.allocateVmExisting.inc();
                            return null;
                        }

                        var workloads = request.getWorkloadList().stream()
                            .map(wl -> Workload.fromProto(wl, Map.of(AllocatorAgent.VM_ALLOCATOR_OTT, vmOtt)))
                            .toList();

                        var initWorkloads = request.getInitWorkloadList().stream()
                            .map(Workload::fromProto)
                            .toList();

                        if (proxyV6Address != null) {
                            initWorkloads = new ArrayList<>(initWorkloads);
                            try {
                                initWorkloads.add(
                                    tunnelAllocator.createRequestTunnelWorkload(
                                        request.getProxyV6Address(), request.getPoolLabel(), request.getZone()
                                    )
                                );
                            } catch (InvalidConfigurationException e) {
                                LOG.error("Error while allocating: {}", e.getMessage(), e);
                                var status = com.google.rpc.Status.newBuilder()
                                    .setCode(Status.INVALID_ARGUMENT.getCode().value())
                                    .setMessage(e.getMessage())
                                    .build();
                                operationsDao.updateError(op.id(), status.toByteArray(), transaction);
                                transaction.commit();
                                responseObserver.onError(e);
                                return null;
                            }
                        }

                        final List<VolumeRequest> volumes;
                        try {
                            volumes = getVolumeRequests(request, transaction);
                        } catch (StatusException e) {
                            var status = com.google.rpc.Status.newBuilder()
                                .setCode(e.getStatus().getCode().value())
                                .setMessage(Objects.toString(e.getStatus().getDescription(), ""))
                                .build();
                            operationsDao.updateError(op.id(), status.toByteArray(), transaction);
                            transaction.commit();
                            responseObserver.onError(e);
                            return null;
                        }

                        var vmSpec = vmDao.create(
                            request.getSessionId(), request.getPoolLabel(), request.getZone(), initWorkloads, workloads,
                            volumes, op.id(), startedAt, proxyV6Address, transaction);

                        var meta = Any.pack(AllocateMetadata.newBuilder()
                            .setVmId(vmSpec.vmId())
                            .build());

                        op.modifyMeta(meta);
                        operationsDao.updateMeta(op.id(), meta.toByteArray(), transaction);

                        final var vmState = new Vm.VmStateBuilder()
                            .setStatus(Vm.VmStatus.CONNECTING)
                            .setAllocationDeadline(Instant.now().plus(config.getAllocationTimeout()))
                            .build();
                        vmDao.update(vmSpec.vmId(), vmState, transaction);

                        transaction.commit();
                        metrics.allocateVmNew.inc();

                        responseObserver.onNext(op.toProto());
                        responseObserver.onCompleted();

                        return vmSpec;
                    }
                });
        } catch (StatusRuntimeException e) {
            failOperation(operationsDao, op.id(), e.getStatus(), e.getStatus().getDescription());
            responseObserver.onError(e);
            return;
        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            var status = Status.INTERNAL.withDescription("Error while executing request").withCause(ex);
            failOperation(operationsDao, op.id(), status, status.getDescription());

            metrics.allocationError.inc();

            responseObserver.onError(status.asException());
            return;
        }

        if (spec == null) {
            return;
        }

        // TODO: do in another thread, don't occupy grpc-pool
        try {
            var vmSubj = subjectClient.createSubject(AuthProvider.INTERNAL, spec.vmId(), SubjectType.VM,
                SubjectCredentials.ott("main", vmOtt, Duration.ofMinutes(15)));
            LOG.info("Create IAM VM subject {} with OTT credentials for vmId {}", vmSubj.id(), spec.vmId());

            withRetries(LOG, () -> vmDao.setVmSubjectId(spec.vmId(), vmSubj.id(), null));

            try {
                var timer = metrics.allocateDuration.startTimer();
                if (proxyV6Address != null) {
                    tunnelAllocator.allocateTunnel(spec);
                }
                allocator.allocate(spec);
                timer.close();
            } catch (InvalidConfigurationException e) {
                LOG.error("Error while allocating: {}", e.getMessage(), e);
                metrics.allocationError.inc();
                failOperation(operationsDao, op.id(), Status.INVALID_ARGUMENT, e.getMessage());
            }
        } catch (Exception e) {
            LOG.error("Error during allocation: {}", e.getMessage(), e);
            metrics.allocationError.inc();
            failOperation(operationsDao, op.id(), Status.INTERNAL, "Error while executing request");
        }
    }

    private static void failOperation(OperationDao operations, String operationId, Status error, String message) {
        var status = com.google.rpc.Status.newBuilder()
            .setCode(error.getCode().value())
            .setMessage(message)
            .build();
        try {
            var op = withRetries(LOG, () -> operations.updateError(operationId, status.toByteArray(), null));
            if (op == null) {
                LOG.error("Cannot fail operation {} with reason {}: operation not found",
                    operationId, message);
            }
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                operationId, message, ex.getMessage(), ex);
        }
    }

    private List<VolumeRequest> getVolumeRequests(AllocateRequest request, TransactionHandle transaction)
        throws SQLException, StatusException
    {
        final List<VolumeRequest> volumes = new ArrayList<>();
        for (var volume : request.getVolumesList()) {
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
                    throw Status.NOT_FOUND.withDescription(message).asException();
                }

                volumes.add(new VolumeRequest(new DiskVolumeDescription(
                    volume.getName(), diskVolume.getDiskId(), disk.spec().sizeGb()
                )));
            } else if (volume.hasNfsVolume()) {
                final var nfsVolume = volume.getNfsVolume();
                volumes.add(new VolumeRequest(new NFSVolumeDescription(
                    volume.getName(), nfsVolume.getServer(), nfsVolume.getShare(), nfsVolume.getCapacity(),
                    nfsVolume.getMountOptionsList()
                )));
            } else {
                final String message = "Unknown volumeType %s for volume=%s"
                    .formatted(volume.getVolumeTypeCase(), volume.getName());
                LOG.error(message);
                throw Status.INVALID_ARGUMENT.withDescription(message).asException();
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
                    try (var tx = TransactionHandle.create(storage)) {
                        var vm = vmDao.get(request.getVmId(), tx);
                        if (vm == null) {
                            LOG.error("Cannot find vm {}", request.getVmId());
                            return Status.NOT_FOUND.withDescription("Cannot find vm");
                        }

                        // TODO(artolord) validate that client can free this vm
                        if (vm.status() != Vm.VmStatus.RUNNING) {
                            LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.state());
                            return Status.FAILED_PRECONDITION.withDescription("State is " + vm.state());
                        }

                        var session = sessionsDao.get(vm.sessionId(), tx);
                        if (session == null) {
                            LOG.error("Corrupted vm with incorrect session id: {}", vm);
                            return Status.INTERNAL;
                        }

                        vmDao.release(vm.vmId(), Instant.now().plus(session.cachePolicy().minIdleTimeout()), tx);

                        tx.commit();
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

    private boolean loadExistingOp(ai.lzy.longrunning.Operation.IdempotencyKey idempotencyKey,
                                   StreamObserver<Operation> responseObserver)
    {
        try {
            var op = withRetries(LOG, () -> operationsDao.getByIdempotencyKey(idempotencyKey.token(), null));
            if (op != null) {
                if (!idempotencyKey.equals(op.idempotencyKey())) {
                    LOG.error("Idempotency key {} conflict", idempotencyKey.token());
                    responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("IdempotencyKey conflict").asException());
                    return true;
                }

                responseObserver.onNext(op.toProto());
                responseObserver.onCompleted();
                return true;
            }

            return false; // key doesn't exist
        } catch (Exception ex) {
            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return true;
        }
    }

    private static boolean validateRequest(CreateSessionRequest request, StreamObserver<Operation> response) {
        if (request.getOwner().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("Owner is not provided").asException());
            return false;
        }
        if (!request.hasCachePolicy() || !request.getCachePolicy().hasIdleTimeout()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("Cache policy is not properly set").asException());
            return false;
        }
        return true;
    }

    private static boolean validateRequest(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> resp) {
        if (request.getSessionId().isBlank()) {
            resp.onError(Status.INVALID_ARGUMENT.withDescription("session_id not set").asException());
            return false;
        }
        return true;
    }

    private static boolean validateRequest(AllocateRequest request, StreamObserver<Operation> response) {
        if (request.getSessionId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("session_id not set").asException());
            return false;
        }
        if (request.getPoolLabel().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("pool_label not set").asException());
            return false;
        }
        if (request.getZone().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("zone not set").asException());
            return false;
        }
        return true;
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
