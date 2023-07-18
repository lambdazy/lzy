package ai.lzy.graph.test.mocks;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

import ai.lzy.graph.GraphExecutor;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.services.AllocatorService;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.MetadataConstants;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Primary;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

@Singleton
@Primary
public class AllocatorServiceMock implements AllocatorService {
    private final ServiceConfig config;
    private final LocalOperationService opService;
    private final Set<Server> servers = new HashSet<>();
    private final ConcurrentHashMap<HostAndPort, LongRunningServiceGrpc.LongRunningServiceBlockingStub> clients =
        new ConcurrentHashMap<>();
    private final BlockingQueue<ManagedChannel> channels = new LinkedBlockingQueue<>();

    public static volatile Consumer<String> onAllocate = (a) -> {};
    public static volatile Consumer<String> onCreateSession = (a) -> {};
    public static volatile Consumer<String> onFree = (a) -> {};
    public static volatile Consumer<String> onGetResponse = (a) -> {};
    public static volatile Consumer<String> onAddCredentials = (a) -> {};
    public static volatile Consumer<String> onCreateWorker = (a) -> {};
    public static volatile Function<LWS.ExecuteRequest, Boolean> onExecute = (a) -> true;

    public AllocatorServiceMock(ServiceConfig config) {
        this.config = config;
        opService = new LocalOperationService("name");
        var server = GrpcUtils.newGrpcServer(HostAndPort.fromString(config.getAllocatorAddress()), GrpcUtils.NO_AUTH)
            .addService(opService)
            .build();

        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        servers.add(server);
    }

    @PreDestroy
    public void close() {
        for (var server: servers) {
            server.shutdown();
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                // ignored
            }
            server.shutdownNow();
        }
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
        onAllocate.accept(sessionId);

        var addr = HostAndPort.fromString("localhost:9090");
        var port = addr.getPort();
        var host = addr.getHost();

        var vmId = UUID.randomUUID().toString();

        var resp = VmAllocatorApi.AllocateResponse.newBuilder()
            .setVmId(vmId)
            .setPoolId("s")
            .setSessionId(sessionId)
            .putMetadata(MetadataConstants.API_PORT, String.valueOf(port))
            .addEndpoints(VmAllocatorApi.AllocateResponse.VmEndpoint.newBuilder()
                .setType(VmAllocatorApi.AllocateResponse.VmEndpoint.VmEndpointType.INTERNAL_IP)
                .setValue(host)
                .build())
            .build();

        var op = ai.lzy.longrunning.Operation.create("test", "", null, VmAllocatorApi.AllocateMetadata.newBuilder()
            .setVmId(vmId)
            .build());
        opService.registerOperation(op);
        opService.updateResponse(op.id(), resp);

        return op.toProto();
    }

    @Override
    public String createSession(String userId, String workflowName, String idempotencyKey) {
        onCreateSession.accept(userId);
        return "session-id";
    }

    @Override
    public void free(String vmId) {
        onFree.accept(vmId);
    }

    @Override
    public VmAllocatorApi.AllocateResponse getResponse(String allocOperationId) {
        onGetResponse.accept(allocOperationId);
        LocalOperationService.OperationSnapshot snapshot = opService.get(allocOperationId);
        try {
            return snapshot.response().unpack(VmAllocatorApi.AllocateResponse.class);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void addCredentials(String vmId, String publicKey, String resourceId) {
        onAddCredentials.accept(vmId);
    }

    @Override
    public WorkerApiGrpc.WorkerApiBlockingStub createWorker(HostAndPort hostAndPort) {
        onCreateWorker.accept(hostAndPort.toString());

        WorkerImpl impl = new WorkerImpl();
        var server = NettyServerBuilder.forPort(hostAndPort.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl)
            .addService(opService)
            .build();
        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servers.add(server);

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

    private class WorkerImpl extends WorkerApiGrpc.WorkerApiImplBase {

        @Override
        public void init(LWS.InitRequest request, StreamObserver<LWS.InitResponse> responseObserver) {
            responseObserver.onNext(LWS.InitResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void execute(LWS.ExecuteRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
            var op = Operation.create("test", "", null, null);
            opService.registerOperation(op);

            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();

            var success = onExecute.apply(request);
            if (success) {
                opService.updateResponse(op.id(), LWS.ExecuteResponse.newBuilder()
                    .setRc(0)
                    .setDescription("Ok")
                    .build());
            } else {
                opService.updateResponse(op.id(), LWS.ExecuteResponse.newBuilder()
                    .setRc(1)
                    .setDescription("Fail")
                    .build());
            }
        }
    }
}
