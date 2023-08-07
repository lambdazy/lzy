package ai.lzy.graph.services.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static ai.lzy.graph.GraphExecutor.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerServiceImpl implements WorkerService {
    private static final Logger LOG = LogManager.getLogger(WorkerServiceImpl.class);

    private final ManagedChannel iamChannel;
    private final RenewableJwt internalUserToken;
    private final ManagedChannel allocatorChannel;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorStub;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub allocatorOpsStub;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final VmAllocatorApi.AllocateRequest.Workload workerWorkload;

    private record WorkerClients(
        ManagedChannel channel,
        WorkerApiGrpc.WorkerApiBlockingStub stub,
        LongRunningServiceGrpc.LongRunningServiceBlockingStub opsStub,
        AtomicReference<Instant> lastAccessTime
    ) {}

    private final Map<String, WorkerClients> workers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService workersCleaner = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "workers-cleaner"));

    public WorkerServiceImpl(ServiceConfig config) {
        var authConfig = config.getIam();
        internalUserToken = authConfig.createRenewableToken();
        Supplier<String> auth = () -> internalUserToken.get().token();

        iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
        allocatorChannel = newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);

        allocatorStub = newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorChannel), APP, auth);
        allocatorOpsStub = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(allocatorChannel), APP, auth);

        subjectClient = new SubjectServiceGrpcClient(APP, iamChannel, internalUserToken::get);
        abClient = new AccessBindingServiceGrpcClient(APP, iamChannel, internalUserToken::get);

        // prepare Worker workload
        {
            var args = new ArrayList<String>(10);

            args.add("--channel-manager");
            args.add(config.getChannelManagerAddress());

            args.add("-i");
            args.add(config.getIam().getAddress());

            args.add("--kafka-bootstrap");
            args.add(String.join(",", config.getKafka().getBootstrapServers()));

            if (config.getKafka().isTlsEnabled()) {
                try (var file = new FileInputStream(config.getKafka().getTlsTruststorePath())) {
                    var bytes = file.readAllBytes();

                    args.add("--truststore-base64");
                    args.add(Base64.getEncoder().encodeToString(bytes));

                    args.add("--truststore-password");
                    args.add(config.getKafka().getTlsTruststorePassword());
                } catch (IOException e) {
                    LOG.error("Cannot deserialize kafka CA", e);
                    throw new RuntimeException("Cannot deserialize kafka CA", e);
                }
            }

            workerWorkload = VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                .setName("worker")
                .setImage(config.getWorkerImage())
                .addAllArgs(args)
                .build();
        }

        workersCleaner.scheduleWithFixedDelay(() -> {
            var deadline = Instant.now().minus(Duration.ofMinutes(30));

            var iter = workers.entrySet().iterator();
            while (iter.hasNext()) {
                var worker = iter.next();
                if (worker.getValue().lastAccessTime.get().isAfter(deadline)) {
                    LOG.info("Delete connection to outdated worker (vmId: {})", worker.getKey());
                    try {
                        worker.getValue().channel().shutdown();
                    } catch (Exception e) {
                        LOG.error("Failed to shutdown channel to VM {}: {}", worker.getKey(), e.getMessage());
                    }
                    iter.remove();
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void shutdown() {
        workersCleaner.shutdownNow();
        allocatorChannel.shutdown();
        iamChannel.shutdown();
        workers.values().forEach(w -> w.channel.shutdown());

        try {
            allocatorChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }

        try {
            iamChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }

        workers.values().forEach(w -> {
            try {
                w.channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        });
    }

    @Override
    public LongRunning.Operation allocateVm(String sessionId, LMO.Requirements requirements, String idempotencyKey) {
        var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setSessionId(sessionId)
            .setPoolLabel(requirements.getPoolLabel())
            .setZone(requirements.getZone())
            .addWorkload(workerWorkload)
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
            .build();

        return withIdempotencyKey(allocatorStub, idempotencyKey).allocate(request);
    }

    @Override
    public void freeVm(String vmId) {
        try {
            allocatorStub.free(VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(vmId)
                .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND ||
                e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION)
            {
                return;
            }
            LOG.error("Cannot free VM {}: {}", vmId, e.getStatus());
            throw e;
        }

        var worker = workers.get(vmId);
        if (worker != null) {
            worker.lastAccessTime.set(Instant.now());
        }
    }

    @Nullable
    @Override
    public LongRunning.Operation getAllocOp(String opId) {
        try {
            return allocatorOpsStub.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(opId)
                .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public Subject createWorkerSubject(String vmId, String publicKey, String resourceId) {
        Subject subj;

        try {
            subj = subjectClient.createSubject(AuthProvider.INTERNAL, vmId, SubjectType.WORKER,
                new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));
        } catch (AuthUniqueViolationException e) {
            subj = subjectClient.findSubject(AuthProvider.INTERNAL, vmId, SubjectType.WORKER);

            boolean done = false;
            for (int i = 0; i < 1000; ++i) {
                try {
                    subjectClient.addCredentials(
                        subj.id(), SubjectCredentials.publicKey("main-" + i, publicKey));
                    done = true;
                    break;
                } catch (AuthUniqueViolationException ex) {
                    // (uid, key-name) already exists, try another key name
                    LOG.error("Credentials for ({}, main-{}) already exist, try next...", vmId, i);
                }
            }

            if (!done) {
                throw new IllegalStateException("Cannot add credentials to the Worker " + vmId);
            }
        }

        try {
            abClient.setAccessBindings(new Workflow(resourceId), List.of(new AccessBinding(Role.LZY_WORKER, subj)));
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                throw e;
            }
        } catch (AuthUniqueViolationException e) {
            // Skipping already exists, it can be from cache
        }

        return subj;
    }

    @Override
    public void init(String vmId, String userId, String workflowName, String host, int port, String workerPrivateKey) {
        restoreWorker(vmId, host, port);
        var worker = requireNonNull(workers.get(vmId));

        worker.stub.init(LWS.InitRequest.newBuilder()
            .setUserId(userId)
            .setWorkflowName(workflowName)
            .setWorkerSubjectName(vmId)
            .setWorkerPrivateKey(workerPrivateKey)
            .build());
    }

    @Nullable
    @Override
    public LongRunning.Operation execute(String vmId, LWS.ExecuteRequest request, String idempotencyKey) {
        var worker = workers.get(vmId);
        if (worker == null) {
            LOG.error("Unknown VM {}", vmId);
            return null;
        }

        worker.lastAccessTime.set(Instant.now());

        return withIdempotencyKey(worker.stub, idempotencyKey).execute(request);
    }

    @Nullable
    @Override
    public LongRunning.Operation getWorkerOp(String vmId, String opId) {
        var worker = workers.get(vmId);
        if (worker == null) {
            LOG.error("Unknown VM {}", vmId);
            return null;
        }

        worker.lastAccessTime.set(Instant.now());

        try {
            return worker.opsStub.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(opId)
                .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public void restoreWorker(String vmId, String host, int port) {
        workers.computeIfAbsent(vmId, __ -> {
            var address = HostAndPort.fromParts(host, port);
            var ch = newGrpcChannel(address, WorkerApiGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME);
            Supplier<String> tokenProvider = () -> internalUserToken.get().token();
            return new WorkerClients(
                ch,
                newBlockingClient(WorkerApiGrpc.newBlockingStub(ch), APP, tokenProvider),
                newBlockingClient(LongRunningServiceGrpc.newBlockingStub(ch), APP, tokenProvider),
                new AtomicReference<>(Instant.now()));
        });
    }
}
