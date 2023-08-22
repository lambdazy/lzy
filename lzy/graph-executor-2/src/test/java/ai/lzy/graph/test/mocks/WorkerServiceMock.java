package ai.lzy.graph.test.mocks;

import ai.lzy.graph.GraphExecutor;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.Worker;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.OperationSnapshot;
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
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static java.util.Optional.ofNullable;

@Singleton
@Primary
public class WorkerServiceMock implements WorkerService {

    private final ServiceConfig config;
    private final LocalOperationService opService;
    private final Server opServer;

    private record WorkerDecs(
        Server server,
        ManagedChannel channel,
        WorkerApiGrpc.WorkerApiBlockingStub stub,
        LongRunningServiceGrpc.LongRunningServiceBlockingStub opsStub
    ) {}

    private final Map<String, WorkerDecs> workers = new ConcurrentHashMap<>();

    public static volatile Consumer<String> onAllocate = (a) -> {};
    public static volatile Consumer<String> onFree = (a) -> {};
    public static volatile Consumer<String> onAddCredentials = (a) -> {};
    public static volatile Consumer<String> onCreateWorker = (a) -> {};
    public static volatile Function<LWS.ExecuteRequest, Boolean> onExecute = (a) -> true;

    public WorkerServiceMock(ServiceConfig config) {
        this.config = config;
        this.opService = new LocalOperationService("ops");

        this.opServer = newGrpcServer(HostAndPort.fromString(config.getAllocatorAddress()), GrpcUtils.NO_AUTH)
            .addService(opService)
            .build();

        try {
            opServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void close() {
        opServer.shutdown();
        workers.values().forEach(w -> {
            w.server.shutdown();
            w.channel.shutdown();
        });
    }

    @Override
    public LongRunning.Operation allocateVm(String sessionId, LMO.Requirements requirements, String idempotencyKey) {
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
    public void freeVm(String vmId) {
        onFree.accept(vmId);
    }

    @Nullable
    @Override
    public LongRunning.Operation getAllocOp(String opId) {
        return ofNullable(opService.get(opId)).map(OperationSnapshot::toProto).orElse(null);
    }

    @Nullable
    @Override
    public LongRunning.Operation cancelAllocOp(String opId, String reason) {
        return ofNullable(opService.cancel(opId, reason)).map(OperationSnapshot::toProto).orElse(null);
    }

    @Override
    public Subject createWorkerSubject(String vmId, String publicKey, String resourceId) {
        onAddCredentials.accept(vmId);
        return new Worker(vmId, AuthProvider.INTERNAL, vmId);
    }

    @Override
    public void init(String vmId, String userId, String workflowName, String host, int port, String workerPrivateKey) {
        onCreateWorker.accept(host + ":" + port);

        WorkerImpl impl = new WorkerImpl();
        var server = NettyServerBuilder.forPort(port)
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

        var workerChannel = newGrpcChannel(host, port, WorkerApiGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME);

        var opsStub = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(workerChannel),
            "worker", () -> config.getIam().createRenewableToken().get().token());

        var stub = newBlockingClient(WorkerApiGrpc.newBlockingStub(workerChannel),
            GraphExecutor.APP, () -> config.getIam().createRenewableToken().get().token());

        workers.put(vmId, new WorkerDecs(server, workerChannel, stub, opsStub));
    }

    @Nullable
    @Override
    public LongRunning.Operation execute(String vmId, LWS.ExecuteRequest request, String idempotencyKey) {
        return null;
    }

    @Nullable
    @Override
    public LongRunning.Operation getWorkerOp(String vmId, String opId) {
        return ofNullable(opService.get(opId)).map(OperationSnapshot::toProto).orElse(null);
    }

    @Nullable
    @Override
    public LongRunning.Operation cancelWorkerOp(String vmId, String opId) {
        return null;
    }

    @Override
    public void restoreWorker(String vmId, String host, int port) {
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
