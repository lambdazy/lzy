package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.AllocateVmAction;
import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.alloc.DeleteSessionAction;
import ai.lzy.allocator.alloc.MountDynamicDiskAction;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.util.AllocatorUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.Errors;
import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.*;
import ai.lzy.v1.VolumeApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.NODE_INSTANCE_ID_KEY;
import static ai.lzy.allocator.model.HostPathVolumeDescription.HostPathType;
import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

@Singleton
@Requires(notEnv = "test-mock")
public class AllocatorService extends AllocatorGrpc.AllocatorImplBase {
    private static final Logger LOG = LogManager.getLogger(AllocatorService.class);

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final DiskDao diskDao;
    private final SessionDao sessionsDao;
    private final AllocationContext allocationContext;
    private final ServiceConfig config;
    private final ServiceConfig.CacheLimits cacheConfig;
    private final ServiceConfig.MountConfig mountConfig;
    private final IdGenerator idGenerator;

    @Inject
    public AllocatorService(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                            SessionDao sessionsDao, DiskDao diskDao, AllocationContext allocationContext,
                            ServiceConfig config, ServiceConfig.CacheLimits cacheConfig,
                            ServiceConfig.MountConfig mountConfig,
                            @Named("AllocatorIdGenerator") IdGenerator idGenerator)
    {
        this.vmDao = vmDao;
        this.operationsDao = operationsDao;
        this.sessionsDao = sessionsDao;
        this.diskDao = diskDao;
        this.allocationContext = allocationContext;
        this.config = config;
        this.cacheConfig = cacheConfig;
        this.mountConfig = mountConfig;
        this.idGenerator = idGenerator;

    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown AllocatorService, active allocations: ???");
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

        final var operationId = idGenerator.generate("create-session-");
        final var sessionId = idGenerator.generate("sid-");

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
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
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

            allocationContext.metrics().createSessionError.inc();

            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(DeleteSessionRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        var reqid = ofNullable(GrpcHeaders.getRequestId()).orElse("unknown");

        Pair<Operation, DeleteSessionAction> ret;

        try {
            ret = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var session = sessionsDao.get(request.getSessionId(), tx);
                    if (session == null) {
                        return null;
                    }

                    var op = Operation.create(
                        session.owner(),
                        "Delete session %s".formatted(session.sessionId()),
                        Duration.ofDays(1),
                        idempotencyKey,
                        null);

                    operationsDao.create(op, tx);
                    session = sessionsDao.delete(session.sessionId(), op.id(), reqid, tx);
                    tx.commit();

                    assert op.id().equals(session.deleteOpId());
                    assert reqid.equals(session.deleteReqid());

                    return Pair.of(op, new DeleteSessionAction(session, op.id(), allocationContext, sessionsDao));
                }
            });
        } catch (Exception ex) {
            allocationContext.metrics().deleteSessionError.inc();
            LOG.error("Error while executing `deleteSession` request, sessionId={}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (ret != null) {
            responseObserver.onNext(ret.getKey().toProto());
            responseObserver.onCompleted();

            allocationContext.startNew(ret.getValue());
        } else {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
    }

    @Override
    public void allocate(AllocateRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        LOG.info("Allocation request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final Vm.TunnelSettings tunnelSettings;
        if (request.hasTunnelSettings()) {
            tunnelSettings = validate(request.getTunnelSettings(), responseObserver);
            if (tunnelSettings == null) {
                return;
            }
        } else {
            tunnelSettings = null;
        }

        final Session session;
        try {
            session = withRetries(LOG, () -> sessionsDao.get(request.getSessionId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get session {}: {}", request.getSessionId(), ex.getMessage(), ex);
            allocationContext.metrics().allocationError.inc();
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            LOG.error("Cannot allocate, session {} not found. Request: {}",
                request.getSessionId(), ProtoPrinter.safePrinter().shortDebugString(request));
            allocationContext.metrics().allocationError.inc();
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Session not found").asException());
            return;
        }

        final var op = Operation.create(
            session.owner(),
            "AllocateVM: pool=%s, zone=%s".formatted(request.getPoolLabel(), request.getZone()),
            config.getAllocationTimeout(),
            idempotencyKey,
            AllocateMetadata.getDefaultInstance());

        final var workloads = request.getWorkloadList().stream()
            .map(Workload::fromProto)
            .toList();

        final var initWorkloads = request.getInitWorkloadList().stream()
            .map(Workload::fromProto)
            .collect(Collectors.toList()); // not `.toList()` because we need a modifiable list here

        if (tunnelSettings != null) {
            try {
                var tunnelWl = allocationContext.tunnelAllocator().createRequestTunnelWorkload(
                    tunnelSettings, request.getPoolLabel(), request.getZone());
                initWorkloads.add(tunnelWl);
            } catch (Exception e) {
                allocationContext.metrics().allocationError.inc();
                LOG.error("Error while allocating tunnel with settings {}: {}", tunnelSettings, e.getMessage(), e);
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Cannot allocate tunnel for proxy %s: %s"
                        .formatted(tunnelSettings, e.getMessage()))
                    .asException());
                return;
            }
        }

        InjectedFailures.failAllocateVm10();

        Runnable allocateCont = null;
        try {
            allocateCont = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    final var volumes = prepareVolumeRequests(request.getVolumesList(), tx);

                    var clusterType = switch (request.getClusterType()) {
                        case USER -> ClusterRegistry.ClusterType.User;
                        case SYSTEM -> ClusterRegistry.ClusterType.System;
                        case UNRECOGNIZED, UNSPECIFIED -> throw Status.INVALID_ARGUMENT
                            .withDescription("Cluster type not specified").asRuntimeException();
                    };

                    final var vmSpec = new Vm.Spec(
                        "VM ID Placeholder",
                        request.getSessionId(),
                        request.getPoolLabel(),
                        request.getZone(),
                        initWorkloads,
                        workloads,
                        volumes,
                        tunnelSettings,
                        clusterType);

                    final var existingVm = vmDao.acquire(vmSpec, tx);
                    if (existingVm != null) {
                        LOG.info("Found existing VM {}", existingVm);

                        op.modifyMeta(
                            AllocateMetadata.newBuilder()
                                .setVmId(existingVm.vmId())
                                .build());

                        var meta = existingVm.allocateState().allocatorMeta();
                        var vmInstanceId = meta == null ? "null" : meta.get(NODE_INSTANCE_ID_KEY);

                        var builder = AllocateResponse.newBuilder()
                            .setSessionId(existingVm.sessionId())
                            .setPoolId(existingVm.poolLabel())
                            .setVmId(existingVm.vmId())
                            .putAllMetadata(requireNonNull(existingVm.runState()).vmMeta())
                            .putMetadata(NODE_INSTANCE_ID_KEY, vmInstanceId)
                            .setFromCache(true);

                        for (var endpoint : existingVm.instanceProperties().endpoints()) {
                            builder.addEndpoints(endpoint.toProto());
                        }

                        op.completeWith(builder.build());

                        operationsDao.create(op, tx);
                        sessionsDao.touch(session.sessionId(), tx);

                        tx.commit();

                        var now = Instant.now();

                        allocationContext.metrics().allocateVmFromCache.inc();
                        allocationContext.metrics().allocateFromCacheDuration
                            .observe(Duration.between(op.createdAt(), now).getSeconds());

                        allocationContext.metrics().runningVms.labels(existingVm.poolLabel()).inc();
                        allocationContext.metrics().cachedVms.labels(existingVm.poolLabel()).dec();
                        allocationContext.metrics().cachedVmsTime.labels(existingVm.poolLabel())
                            .inc(Duration.between(existingVm.idleState().idleSice(), now).getSeconds());

                        responseObserver.onNext(op.toProto());
                        responseObserver.onCompleted();
                        return null;
                    }

                    // create new VM

                    final var vmAllocState = new Vm.AllocateState(
                        op.id(),
                        op.createdAt(),
                        op.deadline(),
                        allocationContext.selfWorkerId(),
                        ofNullable(GrpcHeaders.getRequestId()).orElse("unknown"),
                        /* vmOtt */ UUID.randomUUID().toString(),  // TODO: add expired_at
                        null,
                        null);


                    operationsDao.create(op, tx);
                    sessionsDao.touch(session.sessionId(), tx);
                    final var newVm = vmDao.create(vmSpec, vmAllocState, tx);

                    var meta = Any.pack(AllocateMetadata.newBuilder()
                        .setVmId(newVm.vmId())
                        .build());

                    op.modifyMeta(meta);
                    operationsDao.updateMeta(op.id(), meta, tx);

                    tx.commit();

                    allocationContext.metrics().allocateVmNew.inc();

                    // we should create this action here to inc `runningAllocation` counter before return the result
                    var cont = new AllocateVmAction(newVm, allocationContext, false);

                    responseObserver.onNext(op.toProto());
                    responseObserver.onCompleted();

                    return cont;
                } catch (StatusException e) {
                    allocationContext.metrics().allocationError.inc();
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

            allocationContext.metrics().allocationError.inc();
            LOG.error("Cannot create allocate vm operation for session {}: {}",
                request.getSessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
        }

        if (allocateCont != null) {
            InjectedFailures.failAllocateVm0();
            allocationContext.startNew(allocateCont);
        }
    }

    @Override
    public void free(FreeRequest request, StreamObserver<FreeResponse> responseObserver) {
        LOG.info("Free request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var reqid = ofNullable(GrpcHeaders.getRequestId()).orElse("unknown");

        Status status;
        try {
            status = withRetries(
                LOG,
                () -> {
                    try (var tx = TransactionHandle.create(allocationContext.storage())) {
                        var vm = vmDao.get(request.getVmId(), tx);
                        if (vm == null) {
                            LOG.error("Cannot find vm {}", request.getVmId());
                            return Status.NOT_FOUND.withDescription("Cannot find vm");
                        }

                        if (vm.status() == Vm.Status.ALLOCATING) {
                            LOG.error("Free vm {} in status ALLOCATING, trying to cancel allocation op {}",
                                vm, vm.allocOpId());

                            operationsDao.fail(
                                vm.allocOpId(), toProto(Status.CANCELLED.withDescription("Unexpected free")), tx);
                            tx.commit();
                            return Status.OK;
                        }

                        if (vm.status() != Vm.Status.RUNNING) {
                            LOG.error("Free vm {} in status {}, expected RUNNING", vm, vm.status());
                            return Status.FAILED_PRECONDITION.withDescription("State is " + vm.status());
                        }

                        var session = sessionsDao.get(vm.sessionId(), tx);
                        if (session == null) {
                            LOG.error("Corrupted vm {} with incorrect session id: {}", vm.vmId(), vm.sessionId());
                            return Status.INTERNAL.withDescription("Session %s not found".formatted(vm.sessionId()));
                        }

                        var cachedVms = vmDao.countCachedVms(vm.spec(), session.owner(), tx);

                        if (cachedVms.atOwner() >= cacheConfig.getUserLimit() ||
                            cachedVms.atSession() >= cacheConfig.getSessionLimit() ||
                            cachedVms.atPoolAndSession() >= cacheConfig.getLimit(vm.spec().poolLabel()))
                        {
                            LOG.info("Vms cache is full ({}), about to delete VM {}...", cachedVms, vm.vmId());

                            var action = allocationContext.createDeleteVmAction(vm, "VMs cache is full", reqid, tx);
                            tx.commit();

                            LOG.info("VM {} scheduled to remove (cache is full)", vm.vmId());

                            allocationContext.startNew(action);
                        } else {
                            if (session.cachePolicy().minIdleTimeout().isZero()) {
                                LOG.info("Free VM {} according to cache policy...", vm.vmId());

                                var action = allocationContext.createDeleteVmAction(vm, "Free VM", reqid, tx);
                                tx.commit();

                                LOG.info("VM {} scheduled to remove", vm.vmId());

                                allocationContext.startNew(action);
                            } else {
                                var cacheDeadline = Instant.now().plus(session.cachePolicy().minIdleTimeout());
                                vmDao.release(vm.vmId(), cacheDeadline, tx);
                                tx.commit();

                                LOG.info("VM {} released to session {} cache until {}",
                                    vm.vmId(), vm.sessionId(), cacheDeadline);

                                allocationContext.metrics().cachedVms.labels(vm.poolLabel()).inc();
                            }
                        }

                        allocationContext.metrics().runningVms.labels(vm.poolLabel()).dec();

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

    @Override
    public void mount(VmAllocatorApi.MountRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!mountConfig.isEnabled()) {
            responseObserver.onError(Status.UNIMPLEMENTED.withDescription("Mount management is not enabled")
                .asException());
            return;
        }
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        LOG.info("Mount request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final Vm vm;
        try {
            vm = withRetries(LOG, () -> vmDao.get(request.getVmId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get vm {}: {}", request.getVmId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (vm == null) {
            LOG.error("Cannot find vm {}", request.getVmId());
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot find vm").asException());
            return;
        }

        if (vm.status() == Vm.Status.DELETING) {
            LOG.error("Cannot mount volume to deleting vm {}", request.getVmId());
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Cannot mount volume to deleting vm")
                .asException());
            return;
        }

        var clusterId = AllocatorUtils.getClusterId(vm);
        if (clusterId == null) {
            LOG.error("Cannot mount volume to vm {} without cluster", request.getVmId());
            responseObserver.onError(Status.FAILED_PRECONDITION
                .withDescription("Cannot mount volume to vm without cluster")
                .asException());
            return;
        }

        final Session session;
        try {
            session = withRetries(LOG, () -> sessionsDao.get(vm.sessionId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get session {}: {}", vm.sessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            LOG.error("Corrupted vm {} with incorrect session id: {}", vm.vmId(), vm.sessionId());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Session %s not found".formatted(vm.sessionId())).asException());
            return;
        }

        try {
            var op = createMountOperation(request, idempotencyKey, session);
            var mountWithAction = createMountAction(request, vm, op, clusterId);
            var dynamicMount = mountWithAction.dynamicMount();
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    checkExistingMounts(vm, dynamicMount, tx);
                    var meta = VmAllocatorApi.MountMetadata.newBuilder()
                        .setMount(dynamicMount.toProto())
                        .build();
                    op.modifyMeta(meta);
                    operationsDao.create(op, tx);
                    allocationContext.dynamicMountDao().create(dynamicMount, tx);
                    sessionsDao.touch(session.sessionId(), tx);
                    tx.commit();
                }
            });
            allocationContext.startNew(mountWithAction.action());
            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            LOG.error("Error while mount // vm {}: {}", request.getVmId(), e.getMessage(), e);
            responseObserver.onError(e);
        } catch (Exception ex) {
            LOG.error("Error while mount // vm {}: {}", request.getVmId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription("Unexpected error: " + ex.getMessage())
                .asException());
        }
    }

    @Override
    public void listMounts(VmAllocatorApi.ListMountsRequest request,
                           StreamObserver<VmAllocatorApi.ListMountsResponse> responseObserver)
    {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        try {
            var dynamicMounts = withRetries(LOG, () -> allocationContext.dynamicMountDao()
                .getByVm(request.getVmId(), null));
            responseObserver.onNext(VmAllocatorApi.ListMountsResponse.newBuilder()
                .addAllMounts(dynamicMounts.stream()
                    .map(DynamicMount::toProto)
                    .collect(Collectors.toList()))
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("Error while list mounts // vm {}", request.getVmId(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while list mounts").asException());
        }
    }

    @Override
    public void unmount(VmAllocatorApi.UnmountRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!mountConfig.isEnabled()) {
            responseObserver.onError(Status.UNIMPLEMENTED.withDescription("Mount management is not enabled")
                .asException());
            return;
        }
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        LOG.info("Unmount request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final DynamicMount mount;
        try {
            mount = withRetries(LOG, () -> allocationContext.dynamicMountDao().get(request.getMountId(), false, null));
        } catch (Exception e) {
            LOG.error("Cannot get mount {}: {}", request.getMountId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while get mount").asException());
            return;
        }
        try {
            validateMountForDeletion(request, mount);
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            return;
        }

        final Vm vm;
        try {
            vm = withRetries(LOG, () -> vmDao.get(mount.vmId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get vm {}: {}", mount.vmId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (vm == null) {
            LOG.error("Cannot find vm {}", mount.vmId());
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot find vm").asException());
            return;
        }

        final Session session;
        try {
            session = withRetries(LOG, () -> sessionsDao.get(vm.sessionId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get session {}: {}", vm.sessionId(), ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        if (session == null) {
            LOG.error("Corrupted vm {} with incorrect session id: {}", vm.vmId(), vm.sessionId());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Session %s not found".formatted(vm.sessionId())).asException());
            return;
        }

        try {
            var action = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var dynamicMount = allocationContext.dynamicMountDao().get(request.getMountId(),
                        true, tx);
                    validateMountForDeletion(request, dynamicMount);
                    var unmountAction = allocationContext.createUnmountAction(vm, dynamicMount,
                        idempotencyKey, session.owner(), tx);
                    sessionsDao.touch(session.sessionId(), tx);
                    tx.commit();
                    return unmountAction;
                }
            });
            allocationContext.startNew(action.getLeft());
            responseObserver.onNext(action.getRight().toProto());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            LOG.error("Error while unmount // vm {}: {}", mount.vmId(), e.getMessage(), e);
            responseObserver.onError(e);
        } catch (Exception e) {
            LOG.error("Error while unmount // vm {}: {}", mount.vmId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Unexpected error: " + e.getMessage())
                .asException());
        }
    }

    private static void validateMountForDeletion(VmAllocatorApi.UnmountRequest request, DynamicMount dynamicMount) {
        if (dynamicMount == null) {
            throw Status.NOT_FOUND
                .withDescription("Mount with id %s not found".formatted(request.getMountId()))
                .asRuntimeException();
        }
        var errors = Errors.create();
        if (dynamicMount.state() != DynamicMount.State.READY) {
            errors.add("Mount with id %s is in wrong state: %s. Expected state: %s".formatted(dynamicMount.id(),
                dynamicMount.state(), DynamicMount.State.READY));
        }
        if (dynamicMount.unmountOperationId() != null) {
            errors.add("Mount with id %s is already unmounting".formatted(dynamicMount.id()));
        }
        if (errors.hasErrors()) {
            throw errors.toStatusRuntimeException(Status.Code.FAILED_PRECONDITION);
        }
    }

    private void checkExistingMounts(Vm vm, DynamicMount dynamicMount, TransactionHandle tx) throws SQLException {
        var vmMounts = allocationContext.dynamicMountDao().getByVm(vm.vmId(), tx);
        var errors = Errors.create();
        for (DynamicMount vmMount : vmMounts) {
            if (vmMount.mountPath().equals(dynamicMount.mountPath())) {
                errors.add("Mount with path %s already exists".formatted(vmMount.mountPath()));
            }
            if (vmMount.mountName().equals(dynamicMount.mountName())) {
                errors.add("Mount with name %s already exists".formatted(vmMount.mountName()));
            }
            if (vmMount.volumeRequest().volumeDescription() instanceof DiskVolumeDescription vmDisk &&
                dynamicMount.volumeRequest().volumeDescription() instanceof DiskVolumeDescription requestDisk &&
                vmDisk.diskId().equals(requestDisk.diskId()))
            {
                errors.add("Disk %s is already mounted".formatted(vmDisk.diskId()));
            }
        }
        if (errors.hasErrors()) {
            throw errors.toStatusRuntimeException(Status.Code.ALREADY_EXISTS);
        }
    }

    private MountWithAction createMountAction(VmAllocatorApi.MountRequest request, Vm vm, Operation op,
                                              String clusterId)
    {
        var type = request.getVolumeTypeCase();
        if (type == VmAllocatorApi.MountRequest.VolumeTypeCase.DISK_VOLUME) {
            var diskVolume = request.getDiskVolume();
            var accessMode = validateAccessMode(diskVolume.getAccessMode());
            var storageClass = validateStorageClass(diskVolume.getStorageClass());
            var id = idGenerator.generate("vm-volume-");
            final var diskVolumeDescription = new DiskVolumeDescription(id, diskVolume.getDiskId(),
                diskVolume.getSizeGb(), accessMode, storageClass);
            var mountName = "disk-" + diskVolume.getDiskId();
            var dynamicMount = DynamicMount.createNew(vm.vmId(), clusterId, mountName,
                mountConfig.getWorkerMountPoint() + "/" + request.getMountPath(),
                new VolumeRequest(id, diskVolumeDescription), op.id(),
                allocationContext.selfWorkerId());
            return new MountWithAction(dynamicMount, new MountDynamicDiskAction(vm, dynamicMount, allocationContext));
        }
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("unsupported volume type: " + type));
    }

    private record MountWithAction(DynamicMount dynamicMount, MountDynamicDiskAction action) {}

    @Nonnull
    private Operation createMountOperation(VmAllocatorApi.MountRequest request,
                                           Operation.IdempotencyKey idempotencyKey,
                                           Session session)
    {
        var type = request.getVolumeTypeCase();
        if (type == VmAllocatorApi.MountRequest.VolumeTypeCase.DISK_VOLUME) {
            var diskId = request.getDiskVolume().getDiskId();
            return Operation.create(
                session.owner(),
                "Mount: disk=%s, vm=%s".formatted(diskId, request.getVmId()),
                config.getMountTimeout(),
                idempotencyKey,
                AllocateMetadata.getDefaultInstance());
        }
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("unsupported volume type: " + type));
    }

    private List<VolumeRequest> prepareVolumeRequests(List<VolumeApi.Volume> volumes, TransactionHandle transaction)
        throws SQLException, StatusException
    {
        final var requests = new ArrayList<VolumeRequest>(volumes.size());

        for (var volume : volumes) {
            final var req = switch (volume.getVolumeTypeCase()) {
                case DISK_VOLUME -> {
                    final var diskVolume = volume.getDiskVolume();
                    final var disk = diskDao.get(diskVolume.getDiskId(), transaction);
                    if (disk == null) {
                        final String message = "Disk with id %s not found".formatted(diskVolume.getDiskId());
                        LOG.error(message);
                        throw Status.NOT_FOUND.withDescription(message).asException();
                    }
                    var accessMode = validateAccessMode(diskVolume.getAccessMode());
                    var storageClass = validateStorageClass(diskVolume.getStorageClass());
                    yield new VolumeRequest(idGenerator.generate("disk-volume-").toLowerCase(Locale.ROOT),
                        new DiskVolumeDescription(volume.getName(), diskVolume.getDiskId(), disk.spec().sizeGb(),
                            accessMode, storageClass));
                }

                case HOST_PATH_VOLUME -> {
                    final var hostPathVolume = volume.getHostPathVolume();
                    final var hostPathType = HostPathType.valueOf(hostPathVolume.getHostPathType().name());
                    yield new VolumeRequest(idGenerator.generate("host-path-volume-").toLowerCase(Locale.ROOT),
                        new HostPathVolumeDescription(volume.getName(), hostPathVolume.getPath(), hostPathType));
                }

                case NFS_VOLUME -> {
                    final var nfsVolume = volume.getNfsVolume();
                    yield new VolumeRequest(idGenerator.generate("nfs-volume-").toLowerCase(Locale.ROOT),
                        new NFSVolumeDescription(volume.getName(), nfsVolume.getServer(),
                            nfsVolume.getShare(), nfsVolume.getReadOnly(), nfsVolume.getMountOptionsList()));
                }

                case VOLUMETYPE_NOT_SET -> {
                    final String message = "Volume type not set for volume %s".formatted(volume.getName());
                    LOG.error(message);
                    throw Status.INVALID_ARGUMENT.withDescription(message).asException();
                }
            };

            requests.add(req);
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

    private static boolean validateRequest(DeleteSessionRequest request, StreamObserver<LongRunning.Operation> resp) {
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

    private static boolean validateRequest(VmAllocatorApi.MountRequest request,
                                           StreamObserver<LongRunning.Operation> response)
    {
        var errors = Errors.create();
        if (request.getVmId().isBlank()) {
            errors.add("vm_id isn't set");
        }
        if (request.getMountPath().isBlank()) {
            errors.add("mount_path isn't set");
        } else {
            try {
                Path.of(request.getMountPath());
            } catch (InvalidPathException e) {
                errors.add("mount_path isn't correct", e);
            }
        }
        switch (request.getVolumeTypeCase()) {
            case DISK_VOLUME -> {
                VolumeApi.DiskVolumeType diskVolume = request.getDiskVolume();
                String diskId = diskVolume.getDiskId();
                if (diskId.isBlank()) {
                    errors.add("disk_volume: disk_id isn't set");
                }
                var sizeGb = diskVolume.getSizeGb();
                if (sizeGb <= 0) {
                    errors.add("disk_volume: size_gb isn't set");
                }
            }
            case VOLUMETYPE_NOT_SET -> errors.add("volume_type isn't set");
        }
        if (errors.hasErrors()) {
            response.onError(errors.toStatusRuntimeException(Status.Code.INVALID_ARGUMENT));
            return false;
        }
        return true;
    }

    private static boolean validateRequest(VmAllocatorApi.UnmountRequest request,
                                           StreamObserver<LongRunning.Operation> response)
    {
        var errors = Errors.create();
        if (request.getMountId().isBlank()) {
            errors.add("mount_id isn't set");
        }
        if (errors.hasErrors()) {
            response.onError(errors.toStatusRuntimeException(Status.Code.INVALID_ARGUMENT));
            return false;
        }
        return true;
    }

    private static boolean validateRequest(VmAllocatorApi.ListMountsRequest request,
                                           StreamObserver<VmAllocatorApi.ListMountsResponse> response)
    {
        if (request.getVmId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("vm_id isn't set").asException());
            return false;
        }
        return true;
    }

    @Nullable
    private Vm.TunnelSettings validate(VmAllocatorApi.TunnelSettings tunnelSettings,
                                       StreamObserver<LongRunning.Operation> response)
    {
        var errors = Errors.create();
        int tunnelIndex = tunnelSettings.getTunnelIndex();
        if (tunnelIndex > 255 || tunnelIndex < 0) {
            errors.add("Tunnel index has invalid value: " + tunnelIndex + ". Allowed range is [0, 255]");
        }
        Inet6Address proxyV6Address = null;
        try {
            InetAddress parsedAddress = Inet6Address.getByName(tunnelSettings.getProxyV6Address());
            if (parsedAddress instanceof Inet6Address) {
                proxyV6Address = (Inet6Address) parsedAddress;
            } else {
                LOG.error("Invalid proxy v6 address {} in allocate request", tunnelSettings.getProxyV6Address());
                errors.add("Address " + tunnelSettings.getProxyV6Address() + " isn't v6!");
            }
        } catch (Exception e) {
            LOG.error("Invalid proxy v6 address {} in allocate request", tunnelSettings.getProxyV6Address());
            errors.add("Invalid proxy v6 address " + tunnelSettings.getProxyV6Address());
        }
        if (errors.hasErrors()) {
            allocationContext.metrics().allocationError.inc();
            response.onError(errors.toStatusRuntimeException(Status.Code.INVALID_ARGUMENT));
            return null;
        }
        return new Vm.TunnelSettings(proxyV6Address, tunnelIndex);
    }

    private Volume.AccessMode validateAccessMode(VolumeApi.DiskVolumeType.AccessMode accessMode) {
        return switch (accessMode) {
            case READ_WRITE_ONCE -> Volume.AccessMode.READ_WRITE_ONCE;
            case READ_WRITE_ONCE_POD -> Volume.AccessMode.READ_WRITE_ONCE_POD;
            case READ_WRITE_MANY -> Volume.AccessMode.READ_WRITE_MANY;
            case READ_ONLY_MANY -> Volume.AccessMode.READ_ONLY_MANY;
            case UNRECOGNIZED -> throw Status.INVALID_ARGUMENT.withDescription("invalid access_mode")
                .asRuntimeException();
            default -> Volume.AccessMode.READ_WRITE_ONCE;
        };
    }

    private DiskVolumeDescription.StorageClass validateStorageClass(
        VolumeApi.DiskVolumeType.StorageClass storageClass
    )
    {
        return switch (storageClass) {
            case STORAGE_CLASS_UNSPECIFIED -> null;
            case HDD -> DiskVolumeDescription.StorageClass.HDD;
            case SSD -> DiskVolumeDescription.StorageClass.SSD;
            case UNRECOGNIZED -> throw Status.INVALID_ARGUMENT.withDescription("invalid storage_class")
                .asRuntimeException();
        };
    }
}
