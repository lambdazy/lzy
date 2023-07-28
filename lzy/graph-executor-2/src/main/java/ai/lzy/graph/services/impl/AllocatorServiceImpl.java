package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutor;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.services.AllocatorService;
import ai.lzy.iam.config.IamClientConfiguration;
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
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class AllocatorServiceImpl implements AllocatorService {
    private static final Logger LOG = LogManager.getLogger(AllocatorServiceImpl.class);
    private final ServiceConfig config;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub opStub;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final ConcurrentHashMap<HostAndPort, LongRunningServiceGrpc.LongRunningServiceBlockingStub> clients =
        new ConcurrentHashMap<>();
    private final BlockingQueue<ManagedChannel> channels = new LinkedBlockingQueue<>();

    public AllocatorServiceImpl(ServiceConfig config) {
        this.config = config;
        IamClientConfiguration authConfig = config.getIam();
        var iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var allocatorChannel = newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME);
        final var credentials = authConfig.createRenewableToken();
        allocator = newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorChannel), GraphExecutor.APP,
            () -> credentials.get().token());

        var opChannel = newGrpcChannel(config.getAllocatorAddress(), LongRunningServiceGrpc.SERVICE_NAME);
        opStub = GrpcUtils.newBlockingClient(LongRunningServiceGrpc.newBlockingStub(opChannel),
            GraphExecutor.APP, () -> credentials.get().token());

        subjectClient = new SubjectServiceGrpcClient(GraphExecutor.APP, iamChannel, credentials::get);
        abClient = new AccessBindingServiceGrpcClient(GraphExecutor.APP, iamChannel, credentials::get);

        channels.addAll(List.of(iamChannel, allocatorChannel, opChannel));
    }

    @PreDestroy
    public void shutdown() {
        for (var channel: channels) {
            channel.shutdown();
        }

        for (var channel: channels) {
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    @Override
    public LongRunning.Operation allocate(String userId, String workflowName, String sessionId,
                                          LMO.Requirements requirements)
    {
        final var args = new java.util.ArrayList<>(List.of(
            "--channel-manager", config.getChannelManagerAddress(),
            "-i", config.getIam().getAddress()
        ));

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
                LOG.error("Cannot serialize kafka CA", e);
                throw new RuntimeException("Cannot serialize kafka CA", e);
            }
        }

        final var workload = VmAllocatorApi.AllocateRequest.Workload.newBuilder()
            .setName("worker")
            .setImage(config.getWorkerImage())
            .addAllArgs(args)
            .build();

        final var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setPoolLabel(requirements.getPoolLabel())
            .setZone(requirements.getZone())
            .setSessionId(sessionId)
            .addWorkload(workload)
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
            .build();

        return allocator.allocate(request);
    }

    @Override
    public String createSession(String userId, String workflowName, String idempotencyKey) {
        var stub = GrpcUtils.withIdempotencyKey(allocator, idempotencyKey);

        final var createSessionOp = stub.createSession(
            VmAllocatorApi.CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setDescription("Worker allocation")
                .setCachePolicy(
                    VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.fromMinutes(10))
                        .build())
                .build());

        if (!createSessionOp.getDone()) {
            LOG.error("Unexpected create session operation state");
            throw new RuntimeException("Unexpected create session operation state");
        }

        String sessionId;
        try {
            sessionId = createSessionOp.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot parse CreateSessionResponse", e);
            throw new RuntimeException(e);
        }

        return sessionId;
    }

    @Override
    public void free(String vmId) {
        allocator.free(VmAllocatorApi.FreeRequest.newBuilder()
            .setVmId(vmId)
            .build());
    }

    @Override
    public VmAllocatorApi.AllocateResponse getResponse(String allocOperationId) {
        var res = opStub.get(LongRunning.GetOperationRequest.newBuilder()
            .setOperationId(allocOperationId)
            .build());

        if (!res.getDone()) {
            return null;
        }

        if (res.hasError()) {
            var err = res.getError();
            LOG.error("Error while allocating vm for operation {}: {}", allocOperationId, err);
            return null;
        }

        try {
            return res.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        } catch (Exception e) {
            LOG.error("Cannot unpack response of allocate op for operation {}", allocOperationId, e);
            return null;
        }
    }

    @Override
    public void addCredentials(String vmId, String publicKey, String resourceId) {
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
            abClient.setAccessBindings(new Workflow(resourceId),
                List.of(new AccessBinding(Role.LZY_WORKER, subj)));
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                throw e;
            }
        } catch (AuthUniqueViolationException e) {
            // Skipping already exists, it can be from cache
        }
    }

    @Override
    public WorkerApiGrpc.WorkerApiBlockingStub createWorker(HostAndPort hostAndPort) {
        var workerChannel = newGrpcChannel(hostAndPort, WorkerApiGrpc.SERVICE_NAME);
        var workerChannel2 = GrpcUtils.newGrpcChannel(hostAndPort, LongRunningServiceGrpc.SERVICE_NAME);
        channels.addAll(List.of(workerChannel, workerChannel2));

        var client = GrpcUtils.newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(workerChannel2),
            "worker", () -> config.getIam().createRenewableToken().get().token());
        clients.put(hostAndPort, client);

        return newBlockingClient(WorkerApiGrpc.newBlockingStub(workerChannel),
            GraphExecutor.APP, () -> config.getIam().createRenewableToken().get().token());
    }

    @Override
    public LongRunningServiceGrpc.LongRunningServiceBlockingStub getWorker(HostAndPort hostAndPort) {
        return clients.get(hostAndPort);
    }
}
