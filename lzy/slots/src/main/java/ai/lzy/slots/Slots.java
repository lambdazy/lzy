package ai.lzy.slots;

import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
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
import java.util.Set;
import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

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

        channelManagerChannel = newGrpcChannel(
            channelManagerAddress, LzyChannelManagerGrpc.SERVICE_NAME, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        channelManager = newBlockingClient(
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel), serviceName, token);

        slotsService = new SlotsService();
        Supplier<Credentials> tokenSupplier = () -> new JwtCredentials(token.get());

        final var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(serviceName, iamChannel));

        var accessClient = new AccessServiceGrpcClient(serviceName, iamChannel);
        var workflowResource = new Workflow(ownerId + "/" + workflowName);
        var internalOnlyAccess =
            new AccessServerInterceptor(
                accessClient, tokenSupplier, Set.of(LzySlotsApiGrpc.getReadMethod()),
                workflowResource, AuthPermission.WORKFLOW_MANAGE
            );

        var workflowAccess = new AccessServerInterceptor(
            accessClient, tokenSupplier, Set.of(LzySlotsApiGrpc.getStartTransferMethod()),
            workflowResource, AuthPermission.WORKFLOW_RUN);

        server = newGrpcServer(slotsApiAddress, auth)
            .addService(ServerInterceptors.intercept(
                slotsService,
                workflowAccess,
                internalOnlyAccess))
            .build();

        server.start();
    }

    public synchronized SlotsExecutionContext context(String requestId, String executionId, String taskId,
                                                      List<LMS.Slot> slots, Map<String, String> slotToChannelMapping)
    {
        var context = new SlotsExecutionContext(fsRoot, slots, slotToChannelMapping, channelManager,
            requestId, executionId, taskId, slotsApiAddress.toString(), token, slotsService);

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
