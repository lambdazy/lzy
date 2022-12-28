package ai.lzy.allocator.services;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi.*;
import ai.lzy.v1.VolumeApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
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
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Named;

import static ai.lzy.allocator.model.HostPathVolumeDescription.HostPathType;
import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static java.util.Objects.requireNonNull;

@Singleton
@Requires(beans = MetricReporter.class, notEnv = "test-mock")
public class AllocatorService extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorService.class);

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final DiskDao diskDao;
    private final SessionDao sessionsDao;
    private final VmAllocator allocator;
    private final TunnelAllocator tunnelAllocator;
    private final ServiceConfig config;
    private final AllocatorDataSource storage;
    private final ScheduledExecutorService executor;
    private final AllocatorMetrics metrics;
    private final SubjectServiceGrpcClient subjectClient;
    private final AtomicInteger runningAllocations = new AtomicInteger(0);

    @Inject
    public AllocatorService(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                            SessionDao sessionsDao, DiskDao diskDao, VmAllocator allocator,
                            TunnelAllocator tunnelAllocator, ServiceConfig config, AllocatorDataSource storage,
                            AllocatorMetrics metrics, @Named("AllocatorExecutor") ScheduledExecutorService executor,
                            @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.vmDao = vmDao;
        this.operationsDao = operationsDao;
        this.sessionsDao = sessionsDao;
        this.diskDao = diskDao;
        this.allocator = allocator;
        this.tunnelAllocator = tunnelAllocator;
        this.config = config;
        this.storage = storage;
        this.metrics = metrics;
        this.executor = executor;
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);

        restoreRunningAllocations();
    }

    private void restoreRunningAllocations() {
        try {
            var vms = vmDao.loadNotCompletedVms(config.getInstanceId(), null);
            if (!vms.isEmpty()) {
                LOG.warn("Found {} not completed allocations on allocator {}", vms.size(), config.getInstanceId());

                vms.forEach(vm -> executor.submit(new AllocateVmAction(vm, true)));
            } else {
                LOG.info("Not completed allocations weren't found.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown AllocatorService, active allocations: {}", runningAllocations.get());
    }

    @VisibleForTesting
    public void testRestart(boolean force) {
        if (!force) {
            shutdown();
        }
        restoreRunningAllocations();
    }

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final var operationId = UUID.randomUUID().toString();
        final var sessionId = UUID.randomUUID().toString();

        final var response = CreateSessionResponse.newBuilder()
            .setSessionId(sessionId)
            .build();

        final var op = Operation.createCompleted(
            operationId,
            request.getOwner(),
            "CreateSession: " + request.getDescription(),
            idempotencyKey,
            /* meta */ null,
            response);

        final var minIdleTimeout = ProtoConverter.fromProto(request.getCachePolicy().getIdleTimeout());
        final var session = new Session(sessionId, request.getOwner(), request.getDescription(),
            new CachePolicy(minIdleTimeout), operationId);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationsDao.create(op, tx);
                    sessionsDao.create(session, tx);
                    tx.commit();
                }
            });
        } catch (Exception ex) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, ex, operationsDao, responseObserver, LOG))
            {
                return;
            }

            metrics.createSessionError.inc();

            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        metrics.activeSessions.inc();

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<DeleteSessionResponse> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        boolean success;
        try {
            success = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var ok = sessionsDao.delete(request.getSessionId(), tx);
                    if (ok) {
                        vmDao.delete(request.getSessionId(), tx);
                    }
                    tx.commit();
                    return ok;
                }
            });
        } catch (Exception ex) {
            metrics.deleteSessionError.inc();
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (success) {
            metrics.activeSessions.dec();
        }

        responseObserver.onNext(DeleteSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        LOG.info("Allocation request {}", ProtoPrinter.safePrinter().shortDebugString(request));
        final var startedAt = Instant.now();
        final var allocDeadline = startedAt.plus(config.getAllocationTimeout());

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final Inet6Address proxyV6Address;
        if (request.hasProxyV6Address()) {
            try {
                proxyV6Address = (Inet6Address) Inet6Address.getByName(request.getProxyV6Address());
            } catch (UnknownHostException e) {
                LOG.error("Invalid proxy v6 address {} in allocate request", request.getProxyV6Address());
                metrics.allocationError.inc();
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
            metrics.allocationError.inc();
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            LOG.error("Cannot allocate, session {} not found. Request: {}",
                request.getSessionId(), ProtoPrinter.safePrinter().shortDebugString(request));
            metrics.allocationError.inc();
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        final var op = Operation.create(
            session.owner(),
            "AllocateVM: pool=%s, zone=%s".formatted(request.getPoolLabel(), request.getZone()),
            idempotencyKey,
            AllocateMetadata.getDefaultInstance());

        final var workloads = request.getWorkloadList().stream()
            .map(Workload::fromProto)
            .toList();

        final var initWorkloads = request.getInitWorkloadList().stream()
            .map(Workload::fromProto)
            .collect(Collectors.toList()); // not `.toList()` because we need a modifiable list here

        if (proxyV6Address != null) {
            try {
                var tunnelWl = tunnelAllocator.createRequestTunnelWorkload(
                    request.getProxyV6Address(), request.getPoolLabel(), request.getZone());
                initWorkloads.add(tunnelWl);
            } catch (InvalidConfigurationException e) {
                metrics.allocationError.inc();
                LOG.error("Error while allocating tunnel for {}: {}", proxyV6Address, e.getMessage(), e);
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Cannot allocate tunnel for proxy %s: %s"
                        .formatted(proxyV6Address, e.getMessage()))
                    .asException());
                return;
            }
        }

        final Vm vm;
        try {
            vm = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    final var volumes = prepareVolumeRequests(request.getVolumesList(), tx);

                    var clusterType = switch (request.getClusterType()) {
                        case USER, UNSPECIFIED, UNRECOGNIZED -> ClusterRegistry.ClusterType.User;
                        case SYSTEM -> ClusterRegistry.ClusterType.System;
                    };

                    final var vmSpec = new Vm.Spec(
                        "VM ID Placeholder",
                        request.getSessionId(),
                        request.getPoolLabel(),
                        request.getZone(),
                        initWorkloads,
                        workloads,
                        volumes,
                        proxyV6Address,
                        clusterType);

                    final var existingVm = vmDao.acquire(vmSpec, tx);
                    if (existingVm != null) {
                        LOG.info("Found existing VM {}", existingVm);

                        op.modifyMeta(
                            AllocateMetadata.newBuilder()
                                .setVmId(existingVm.vmId())
                                .build());

                        op.setResponse(
                            AllocateResponse.newBuilder()
                                .setSessionId(existingVm.sessionId())
                                .setPoolId(existingVm.poolLabel())
                                .setVmId(existingVm.vmId())
                                .putAllMetadata(requireNonNull(existingVm.runState()).vmMeta())
                                .build());

                        operationsDao.create(op, tx);

                        tx.commit();

                        metrics.allocateVmFromCache.inc();
                        metrics.allocateFromCacheDuration.observe(
                            Duration.between(startedAt, Instant.now()).getSeconds());

                        responseObserver.onNext(op.toProto());
                        responseObserver.onCompleted();
                        return null;
                    }

                    // create new VM

                    final var vmOtt = UUID.randomUUID().toString();

                    operationsDao.create(op, tx);
                    final var newVm = vmDao.create(vmSpec, op.id(), startedAt, allocDeadline, vmOtt,
                        config.getInstanceId(), tx);

                    var meta = Any.pack(AllocateMetadata.newBuilder()
                        .setVmId(newVm.vmId())
                        .build());

                    op.modifyMeta(meta);
                    operationsDao.updateMeta(op.id(), meta.toByteArray(), tx);

                    tx.commit();

                    metrics.allocateVmNew.inc();

                    responseObserver.onNext(op.toProto());
                    responseObserver.onCompleted();

                    return newVm;
                } catch (StatusException e) {
                    metrics.allocationError.inc();
                    responseObserver.onError(e);
                    return null;
                }
            });
        } catch (Exception ex) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, ex, operationsDao, responseObserver, LOG))
            {
                return;
            }

            metrics.allocationError.inc();
            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (vm == null) {
            // either use existing vm or smth went wrong
            return;
        }

        InjectedFailures.failAllocateVm1();

        executor.submit(new AllocateVmAction(vm, false));
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        LOG.info("Free request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        final Instant[] cacheDeadline = {null};
        Status status;
        try {
            status = withRetries(
                LOG,
                () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        var vm = vmDao.get(request.getVmId(), tx);
                        if (vm == null) {
                            LOG.error("Cannot find vm {}", request.getVmId());
                            return Status.NOT_FOUND.withDescription("Cannot find vm");
                        }

                        // TODO(artolord) validate that client can free this vm
                        if (vm.status() != Vm.Status.RUNNING) {
                            LOG.error("Freed vm {} in status {}, expected RUNNING", vm, vm.status());
                            return Status.FAILED_PRECONDITION.withDescription("State is " + vm.status());
                        }

                        var session = sessionsDao.get(vm.sessionId(), tx);
                        if (session == null) {
                            LOG.error("Corrupted vm with incorrect session id: {}", vm);
                            return Status.INTERNAL.withDescription("Session %s not found".formatted(vm.sessionId()));
                        }

                        cacheDeadline[0] = Instant.now().plus(session.cachePolicy().minIdleTimeout());

                        vmDao.release(vm.vmId(), cacheDeadline[0], tx);

                        tx.commit();

                        LOG.info("VM {} released to session {} cache until {}",
                            vm.vmId(), vm.sessionId(), cacheDeadline[0]);

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

    private List<VolumeRequest> prepareVolumeRequests(List<VolumeApi.Volume> volumes, TransactionHandle transaction)
        throws SQLException, StatusException
    {
        final var requests = new ArrayList<VolumeRequest>(volumes.size());

        for (var volume : volumes) {
            final var descr = switch (volume.getVolumeTypeCase()) {
                case DISK_VOLUME -> {
                    final var diskVolume = volume.getDiskVolume();
                    final var disk = diskDao.get(diskVolume.getDiskId(), transaction);
                    if (disk == null) {
                        final String message = "Disk with id %s not found".formatted(diskVolume.getDiskId());
                        LOG.error(message);
                        throw Status.NOT_FOUND.withDescription(message).asException();
                    }
                    yield new DiskVolumeDescription("disk-volume-" + UUID.randomUUID(), volume.getName(),
                        diskVolume.getDiskId(), disk.spec().sizeGb());
                }

                case HOST_PATH_VOLUME -> {
                    final var hostPathVolume = volume.getHostPathVolume();
                    final var hostPathType = HostPathType.valueOf(hostPathVolume.getHostPathType().name());
                    yield new HostPathVolumeDescription("host-path-volume-" + UUID.randomUUID(), volume.getName(),
                        hostPathVolume.getPath(), hostPathType);
                }

                case NFS_VOLUME -> {
                    final var nfsVolume = volume.getNfsVolume();
                    yield new NFSVolumeDescription("nfs-volume-" + UUID.randomUUID(), volume.getName(),
                        nfsVolume.getServer(), nfsVolume.getShare(), nfsVolume.getCapacity(),
                        nfsVolume.getMountOptionsList());
                }

                case VOLUMETYPE_NOT_SET -> {
                    final String message = "Volume type not set for volume %s".formatted(volume.getName());
                    LOG.error(message);
                    throw Status.INVALID_ARGUMENT.withDescription(message).asException();
                }
            };

            requests.add(new VolumeRequest(descr));
        }

        return requests;
    }

    private static boolean validateRequest(CreateSessionRequest request,
                                           StreamObserver<LongRunning.Operation> response)
    {
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

    private static boolean validateRequest(AllocateRequest request, StreamObserver<LongRunning.Operation> response) {
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
        if (request.getWorkloadCount() == 0) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("workload not set").asException());
            return false;
        }
        return true;
    }


    private final class AllocateVmAction implements Runnable {
        private Vm vm;
        private final boolean restore;

        AllocateVmAction(Vm vm, boolean restore) {
            this.vm = vm;
            this.restore = restore;
            runningAllocations.getAndIncrement();
        }

        @Override
        public void run() {
            if (restore) {
                LOG.info("Restore VM {} allocation...", vm.toString());
            }

            try {
                InjectedFailures.failAllocateVm2();

                if (vm.allocateState().vmSubjectId() == null) {
                    var vmSubj = subjectClient
                        .withIdempotencyKey(vm.vmId())
                        .createSubject(
                            AuthProvider.INTERNAL,
                            vm.vmId(),
                            SubjectType.VM,
                            SubjectCredentials.ott("main", vm.allocateState().vmOtt(), Duration.ofMinutes(15)));

                    LOG.info("Create IAM VM subject {} with OTT credentials for vmId {}", vmSubj.id(), vm.vmId());

                    withRetries(LOG, () -> vmDao.setVmSubjectId(vm.vmId(), vmSubj.id(), null));

                    vm = vm.withVmSubjId(vmSubj.id());
                }

                InjectedFailures.failAllocateVm3();

                try {
                    if (vm.proxyV6Address() != null) {
                        if (vm.allocateState().tunnelPodName() == null) {
                            var tunnelPodName = tunnelAllocator.allocateTunnel(vm.spec());
                            withRetries(LOG, () -> vmDao.setTunnelPod(vm.vmId(), tunnelPodName, null));
                            vm = vm.withTunnelPod(tunnelPodName);
                        } else {
                            LOG.info("Found existing tunnel pod {} with address {} for VM {}",
                                vm.allocateState().tunnelPodName(), vm.proxyV6Address(), vm.vmId());
                        }

                        InjectedFailures.failAllocateVm4();
                    }

                    allocator.allocate(vm);
                } catch (InvalidConfigurationException e) {
                    LOG.error("Error while allocating: {}", e.getMessage(), e);
                    metrics.allocationError.inc();
                    operationsDao.failOperation(
                        vm.allocOpId(), toProto(Status.INVALID_ARGUMENT.withDescription(e.getMessage())), LOG);
                }
            } catch (InjectedFailures.TerminateException e) {
                LOG.error("Got InjectedFailure exception: " + e.getMessage());
                // don't fail operation explicitly, just pass
            } catch (Exception e) {
                LOG.error("Error during VM {} allocation: {}", vm.vmId(), e.getMessage(), e);
                metrics.allocationError.inc();
                operationsDao.failOperation(
                    vm.allocOpId(), toProto(Status.INTERNAL.withDescription("Error while executing request")), LOG);
            } finally {
                runningAllocations.getAndDecrement();
            }
        }
    }
}
