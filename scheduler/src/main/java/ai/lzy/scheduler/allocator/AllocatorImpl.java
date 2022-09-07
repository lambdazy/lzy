package ai.lzy.scheduler.allocator;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Servant;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.model.Operation;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.OperationServiceApiGrpc.OperationServiceApiBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Duration;
import io.grpc.ManagedChannel;
import io.gsonfire.builders.JsonObjectBuilder;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


@Singleton
public class AllocatorImpl implements ServantsAllocator {
    private static final Logger LOG = LogManager.getLogger(AllocatorImpl.class);
    public static final AtomicBoolean randomServantPorts = new AtomicBoolean(false);

    private final ServiceConfig config;
    private final ServantEventProcessorConfig processorConfig;
    private final ServantMetaStorage metaStorage;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final OperationServiceApiBlockingStub operations;
    private final AtomicInteger testServantCounter = new AtomicInteger(0);
    private final IamClientConfiguration authConfig;
    private final ManagedChannel iamChannel;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    public AllocatorImpl(ServiceConfig config, ServantEventProcessorConfig processorConfig,
                         ServantMetaStorage metaStorage)
    {
        this.config = config;
        this.processorConfig = processorConfig;
        this.metaStorage = metaStorage;
        this.authConfig = config.getIam();
        this.iamChannel = ChannelBuilder
            .forAddress(authConfig.getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();

        final var address = HostAndPort.fromString(config.getAllocatorAddress());
        final var channel = new ChannelBuilder(address.getHost(), address.getPort())
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .usePlaintext()
            .build();
        final var credentials = authConfig.createCredentials();
        allocator = AllocatorGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        final var opChannel = new ChannelBuilder(address.getHost(), address.getPort())
            .enableRetry(OperationServiceApiGrpc.SERVICE_NAME)
            .usePlaintext()
            .build();
        operations = OperationServiceApiGrpc.newBlockingStub(opChannel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        subjectClient = new SubjectServiceGrpcClient(iamChannel, authConfig::createCredentials);
        abClient = new AccessBindingServiceGrpcClient(iamChannel, authConfig::createCredentials);
    }


    @Override
    public void allocate(String workflowName, String servantId, Operation.Requirements requirements) {
        final Credentials credentials;
        try {
            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, servantId, SubjectType.SERVANT);

            final var cred = JwtUtils.generateCredentials(subj.id(), AuthProvider.INTERNAL.name());
            credentials = cred.credentials();

            subjectClient.addCredentials(subj, "main", cred.publicKey(), CredentialsType.PUBLIC_KEY);
            abClient.setAccessBindings(new Workflow(workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for servant", e);
            throw new RuntimeException(e);
        }

        // TODO(artolord) add session caching
        final var session = allocator.createSession(CreateSessionRequest.newBuilder()
            .setOwner("lzy-scheduler")
            .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder()
                .setIdleTimeout(Duration.newBuilder()
                    .setNanos(0)
                    .setSeconds(0)
                    .build())
                .build())
            .build());

        final int port;
        final int fsPort;
        final String mountPoint;

        if (randomServantPorts.get()) {
            port = FreePortFinder.find(10000, 11000);
            fsPort = FreePortFinder.find(11000, 12000);
            mountPoint = "/tmp/lzy" + testServantCounter.incrementAndGet();
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
            "--token", '"' + credentials.token() + '"',
            "--servant-id", servantId,
            "--scheduler-heartbeat-period", processorConfig.executingHeartbeatPeriod().toString()
        );
        final var workload = Workload.newBuilder()
            .setName(servantId)
            .setImage(config.getServantImage())
            .addAllArgs(args)
            .putAllPortBindings(ports)
            .build();

        final var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setPoolLabel(requirements.poolLabel())
            .setZone(requirements.zone())
            .setSessionId(session.getSessionId())
            .addWorkload(workload)
            .build();

        final var op = allocator.allocate(request);
        metaStorage.saveMeta(workflowName, servantId, new KuberMeta(session.getSessionId(), op.getId()).toJson());
    }

    @Override
    public void free(String workflowName, String servantId) throws Exception {

        subjectClient.removeSubject(new Servant(servantId));
        final var s = metaStorage.getMeta(workflowName, servantId);
        if (s == null) {
            LOG.error("Cannot get meta. WfName: {}, servantId: {}", workflowName, servantId);
            throw new Exception("Cannot get meta.");
        }

        final var meta = KuberMeta.fromJson(s);
        if (meta == null) {
            LOG.error("Cannot parse meta {}", s);
            throw new Exception("Cannot parse meta");
        }

        final var op = operations.get(OperationService.GetOperationRequest.newBuilder()
            .setOperationId(meta.opId)
            .build()
        );

        if (!op.getDone()) {
            operations.cancel(OperationService.CancelOperationRequest.newBuilder()
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
