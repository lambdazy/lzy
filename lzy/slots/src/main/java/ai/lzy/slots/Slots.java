package ai.lzy.slots;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowSubjectOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;
import ai.lzy.v1.common.LMS;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Slots {
    private final Path fsRoot;
    private final SlotsService slotsService;
    private final List<SlotsExecutionContext> contexts = new ArrayList<>();  // Guarded by this
    private final Supplier<String> token;
    private final LzyChannelManagerBlockingStub channelManager;
    private final ManagedChannel channelManagerChannel;
    private final HostAndPort slotsApiAddress;
    private final Server server;

    public Slots(Path fsRoot, Supplier<String> token, HostAndPort slotsApiAddress,
                 HostAndPort channelManagerAddress, String serviceName, ManagedChannel iamChannel,
                 String workflowName, String ownerId) throws IOException
    {
        this.fsRoot = fsRoot;
        this.token = token;
        this.slotsApiAddress = slotsApiAddress;

        if (!fsRoot.toFile().exists()) {
            Files.createDirectories(fsRoot);
        }

        channelManagerChannel = GrpcUtils.newGrpcChannel(channelManagerAddress, LzyChannelManagerGrpc.SERVICE_NAME);
        channelManager = GrpcUtils.newBlockingClient(
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel), serviceName, token);

        slotsService = new SlotsService();

        final var authInterceptor = new AuthServerInterceptor(
            new AuthenticateServiceGrpcClient(serviceName, iamChannel));

        var interceptor = AllowSubjectOnlyInterceptor.withPermissions(
            Map.of(new Workflow(ownerId + "/" + workflowName), AuthPermission.WORKFLOW_RUN),
            serviceName, iamChannel);

        server = GrpcUtils.newGrpcServer(slotsApiAddress, authInterceptor)
            .addService(ServerInterceptors.intercept(slotsService, interceptor))
            .build();

        server.start();
    }

    public synchronized SlotsExecutionContext context(String executionId, List<LMS.Slot> slots,
                                                      Map<String, String> slotToChannelMapping)
    {
        var context = new SlotsExecutionContext(fsRoot, slots, slotToChannelMapping, channelManager, executionId,
            slotsApiAddress.toString(), token, slotsService);

        contexts.add(context);

        return context;
    }

    public synchronized void close() {
        server.shutdown();
        channelManagerChannel.shutdown();

        for (var context : contexts) {
            context.close();
        }
    }
}
