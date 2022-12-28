package ai.lzy.scheduler.allocator;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.Worker;
import ai.lzy.model.operation.Operation;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.configs.WorkerEventProcessorConfig;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.gsonfire.builders.JsonObjectBuilder;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


@Singleton
public class AllocatorImpl implements WorkersAllocator {
    private static final Logger LOG = LogManager.getLogger(AllocatorImpl.class);

    public static final String ENV_WORKER_PKEY = "LZY_WORKER_PKEY"; // same as at ai.lzy.worker.Worker
    public static final String ENV_RESOURCE_TYPE = "RESOURCE_TYPE";
    public static final String RESOURCE_TYPE = "lzy.worker";
    public static final String ENV_RESOURCE_ID = "RESOURCE_ID";


    public static final AtomicBoolean randomWorkerPorts = new AtomicBoolean(false);

    private final ServiceConfig config;
    private final WorkerEventProcessorConfig processorConfig;
    private final WorkerMetaStorage metaStorage;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub operations;
    private final AtomicInteger testWorkerCounter = new AtomicInteger(0);
    private final IamClientConfiguration authConfig;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final ManagedChannel iamChannel;
    private final ManagedChannel allocatorChannel;
    private final ManagedChannel opChannel;

    public AllocatorImpl(ServiceConfig config, WorkerEventProcessorConfig processorConfig,
                         WorkerMetaStorage metaStorage)
    {
        this.config = config;
        this.processorConfig = processorConfig;
        this.metaStorage = metaStorage;
        this.authConfig = config.getIam();
        this.iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        allocatorChannel = newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME);
        final var credentials = authConfig.createRenewableToken();
        allocator = newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorChannel), SchedulerApi.APP,
            () -> credentials.get().token());

        opChannel = newGrpcChannel(config.getAllocatorAddress(), LongRunningServiceGrpc.SERVICE_NAME);
        operations = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(opChannel), SchedulerApi.APP,
            () -> credentials.get().token());

        subjectClient = new SubjectServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
        abClient = new AccessBindingServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
    }

    @PreDestroy
    public void shutdown() {
        GrpcChannels.awaitTermination(opChannel, java.time.Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(allocatorChannel, java.time.Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(iamChannel, java.time.Duration.ofSeconds(10), getClass());
    }

    @Override
    public void allocate(String userId, String workflowName, String workerId, Operation.Requirements requirements) {
        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            privateKey = workerKeys.privateKey();

            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, workerId, SubjectType.WORKER,
                new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));

            abClient.setAccessBindings(new Workflow(userId + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for worker", e);
            throw new RuntimeException(e);
        }

        // TODO: use allocator_session_id from LzyWorker
        final var createSessionOp = allocator.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setDescription("Worker allocation, wf='%s', w='%s'".formatted(workflowName, workerId))
                .setCachePolicy(
                    VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.ZERO)
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

        final int port;
        final int fsPort;
        final String mountPoint;

        if (randomWorkerPorts.get()) {
            port = FreePortFinder.find(10000, 11000);
            fsPort = FreePortFinder.find(11000, 12000);
            mountPoint = "/tmp/lzy"; // + testWorkerCounter.incrementAndGet();
        } else {
            port = 9999;
            fsPort = 9988;
            mountPoint = "/tmp/lzy";
        }

        final var ports = Map.of(
            port, port,
            fsPort, fsPort
        );

        final var args = List.of(
            "--workflow-name", workflowName,
            "--port", String.valueOf(port),
            "--fs-port", String.valueOf(fsPort),
            "--scheduler-address", config.getSchedulerAddress(),
            "--channel-manager", config.getChannelManagerAddress(),
            "--lzy-mount", mountPoint,
            "--worker-id", workerId,
            "--user-default-image", config.getUserDefaultImage(),
            "--scheduler-heartbeat-period", processorConfig.executingHeartbeatPeriod().toString()
        );
        final var workload = Workload.newBuilder()
            .setName(workerId)
            .setImage(config.getWorkerImage())
            .putEnv(ENV_WORKER_PKEY, privateKey)
            .putEnv(ENV_RESOURCE_TYPE, RESOURCE_TYPE)
            .putEnv(ENV_RESOURCE_ID, workerId)
            .addAllArgs(args)
            .putAllPortBindings(ports)
            .build();

        final var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setPoolLabel(requirements.poolLabel())
            .setZone(requirements.zone())
            .setSessionId(sessionId)
            .addWorkload(workload)
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
            .build();

        final var op = allocator.allocate(request);
        metaStorage.saveMeta(workflowName, workerId, new KuberMeta(sessionId, op.getId()).toJson());
    }

    @Override
    public void free(String workflowName, String workerId) throws Exception {
        subjectClient.removeSubject(new Worker(workerId));
        final var s = metaStorage.getMeta(workflowName, workerId);
        if (s == null) {
            LOG.error("Cannot get meta. WfName: {}, workerId: {}", workflowName, workerId);
            throw new Exception("Cannot get meta.");
        }

        final var meta = KuberMeta.fromJson(s);
        if (meta == null) {
            LOG.error("Cannot parse meta {}", s);
            throw new Exception("Cannot parse meta");
        }

        final var op = operations.get(LongRunning.GetOperationRequest.newBuilder()
            .setOperationId(meta.opId)
            .build()
        );

        if (!op.getDone()) {
            operations.cancel(LongRunning.CancelOperationRequest.newBuilder()
                .setOperationId(op.getId())
                .build());
            return;
        }
        final var req = VmAllocatorApi.FreeRequest.newBuilder()
            .setVmId(op.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId())
            .build();
        allocator.free(req);

        allocator.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
            .setSessionId(meta.sessionId)
            .build());
    }

    private record KuberMeta(String sessionId, String opId) {
        String toJson() {
            return new JsonObjectBuilder()
                .set("sessionId", sessionId)
                .set("opId", opId)
                .build()
                .toString();
        }

        @Nullable
        static KuberMeta fromJson(String json) {
            final JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            final JsonElement namespace = obj.get("sessionId");
            final JsonElement podName = obj.get("opId");
            if (namespace == null || podName == null) {
                return null;
            }
            return new KuberMeta(namespace.getAsString(), podName.getAsString());
        }
    }
}
