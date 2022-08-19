package ai.lzy.scheduler.allocator;

import ai.lzy.model.Operation;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.OperationServiceApiGrpc.OperationServiceApiBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import com.google.common.net.HostAndPort;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Duration;
import io.gsonfire.builders.JsonObjectBuilder;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.List;

@Singleton
public class AllocatorImpl implements ServantsAllocator {
    private static final Logger LOG = LogManager.getLogger(AllocatorImpl.class);

    private final ServiceConfig config;
    private final ServantMetaStorage metaStorage;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final OperationServiceApiBlockingStub operations;

    public AllocatorImpl(ServiceConfig config, ServantMetaStorage metaStorage) {
        this.config = config;
        this.metaStorage = metaStorage;
        final var address = HostAndPort.fromString(config.allocatorAddress());
        final var channel = new ChannelBuilder(address.getHost(), address.getPort())
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .usePlaintext()
            .build();
        allocator = AllocatorGrpc.newBlockingStub(channel);

        final var opChannel = new ChannelBuilder(address.getHost(), address.getPort())
            .enableRetry(OperationServiceApiGrpc.SERVICE_NAME)
            .usePlaintext()
            .build();
        operations = OperationServiceApiGrpc.newBlockingStub(opChannel);
    }


    @Override
    public void allocate(String workflowName, String servantId, Operation.Requirements requirements) {
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

        final var args = List.of(
            "--scheduler-address", config.schedulerAddress(),
            "--channel-manager-address", config.channelManagerAddress(),
            "--lzy-mount", "/tmp/lzy",
            "--port", "9999",
            "--servant-id", servantId,
            "--workflow-name", workflowName
        );
        final var workload = Workload.newBuilder()
            .setName(servantId)
            .setImage(config.servantImage())
            .addAllArgs(args)
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
            .setOperationId(meta.sessionId)
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
