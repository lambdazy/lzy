package ai.lzy.portal;

import static java.util.Objects.requireNonNull;

import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Servant;
import ai.lzy.v1.Tasks;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

@SuppressWarnings({"ResultOfMethodCallIgnored", "SameParameterValue"})
class ServerMock extends LzyServerGrpc.LzyServerImplBase {
    final int port;
    final ApplicationContext ctx;
    final Server server;
    final Map<String, ServantHandler> servantHandlers = new ConcurrentHashMap<>();

    ServerMock() {
        this.port = GrpcUtils.rollPort();
        Map<String, Object> properties = Map.of(
            "server.server-uri", "grpc://localhost:" + port,
            "portal.allocator-address", "grpc://localhost:" + port
        );
        this.ctx = ApplicationContext.run(properties);
        this.server = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(this))
            .addService(new AllocatorPrivateAPIMock())
            .build();
    }

    int port() {
        return port;
    }

    void startup() throws IOException {
        server.start();
    }

    void stop() throws InterruptedException {
        for (var servant : servantHandlers.values()) {
            servant.shutdown();
            servant.join();
        }
        for (var servant : servantHandlers.values()) {
            servant.awaitTermination(2, TimeUnit.SECONDS);
        }
        server.shutdown();
        server.awaitTermination();
    }

    // ---  SERVER GRPC API  ---
    @Override
    public void getBucket(Lzy.GetBucketRequest request, StreamObserver<Lzy.GetBucketResponse> responseObserver) {
        responseObserver.onNext(Lzy.GetBucketResponse.newBuilder()
            .setBucket("bucket_" + request.getAuth().getTask().getServantId())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getS3Credentials(Lzy.GetS3CredentialsRequest request,
                                 StreamObserver<Lzy.GetS3CredentialsResponse> responseObserver)
    {
        responseObserver.onNext(Lzy.GetS3CredentialsResponse.newBuilder()
            .setAmazon(Lzy.AmazonCredentials.newBuilder()
                .setEndpoint("localhost:12345")
                .setAccessToken("zzz")
                .build())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void registerServant(Lzy.AttachServant request, StreamObserver<Lzy.AttachStatus> responseObserver) {
        System.out.println("register servant: " + JsonUtils.printSingleLine(request));

        var servantHandler = new ServantHandler(request);
        servantHandlers.put(request.getServantId(), servantHandler);

        responseObserver.onNext(Lzy.AttachStatus.getDefaultInstance());
        responseObserver.onCompleted();

        servantHandler.start();
    }

    @Override
    public void zygotes(IAM.Auth request, StreamObserver<Operations.ZygoteList> responseObserver) {
        responseObserver.onNext(Operations.ZygoteList.getDefaultInstance());
        responseObserver.onCompleted();
    }

    // --- SERVER GRPC API  ---

    void start(String servantId, Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> response) {
        servant(servantId).startTask(request, response);
    }

    void waitServantStart(String servantId) throws Exception {
        requireNonNull(servantHandlers.get(servantId)).waitStart();
    }

    ServantHandler servant(String servantId) {
        return servantHandlers.get(servantId);
    }

    void waitTaskCompleted(String servantId, String taskId) throws Exception {
        var taskStatus = servant(servantId).tasks.get(taskId).get();
        Assert.assertEquals(taskStatus.getDescription(), 0, taskStatus.getRc());
    }

    static class ServantHandler extends Thread {
        private final String servantId;
        private final LzyServantGrpc.LzyServantBlockingStub servantStub;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private volatile String taskId = null;
        private final Map<String, CompletableFuture<Servant.ExecutionConcluded>> tasks = new ConcurrentHashMap<>();
        private volatile CompletableFuture<Void> taskCompleted = new CompletableFuture<>();

        ServantHandler(Lzy.AttachServant req) {
            setName(req.getServantId());
            setDefaultUncaughtExceptionHandler((t, e) -> {
                System.err.println("Exception in thread " + t.getName() + ": " + e.getMessage());
                e.printStackTrace(System.err);
            });

            this.servantId = req.getServantId();
            var servantUri = req.getServantURI();
            this.servantStub = LzyServantGrpc.newBlockingStub(
                ChannelBuilder.forAddress(servantUri.substring("servant://".length()))
                    .usePlaintext()
                    .enableRetry(LzyServantGrpc.SERVICE_NAME)
                    .build());
        }

        public void shutdown() {
            servantStub.stop(IAM.Empty.getDefaultInstance());
            ((ManagedChannel) servantStub.getChannel()).shutdown();
        }

        public boolean awaitTermination(long c, TimeUnit timeUnit) throws InterruptedException {
            return ((ManagedChannel) servantStub.getChannel()).awaitTermination(c, timeUnit);
        }

        public void waitStart() throws Exception {
            started.get();
        }

        public void startTask(Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> response) {
            try {
                servantStub.env(Operations.EnvSpec.getDefaultInstance());

                taskId = request.getTid();
                tasks.put(taskId, new CompletableFuture<>());
                taskCompleted = new CompletableFuture<>();

                servantStub.execute(request);

                response.onNext(Tasks.TaskProgress.newBuilder()
                    .setTid(request.getTid())
                    .setStatus(Tasks.TaskProgress.Status.EXECUTING)
                    .setRc(0)
                    .setZygoteName(request.getZygote().getName())
                    .build());
                response.onCompleted();
            } catch (StatusRuntimeException e) {
                response.onError(e);
            }
        }

        @Override
        public void run() {
            System.out.println("Starting servant '" + servantId + "' worker...");
            var servantProgress = servantStub.start(IAM.Empty.getDefaultInstance());
            try {
                servantProgress.forEachRemaining(action -> {
                    System.out.println("--> Servant '" + servantId + "': " + JsonUtils.printSingleLine(action));
                    if (action.hasStart()) {
                        started.complete(null);
                    } else if (action.hasExecuteStop()) {
                        tasks.get(taskId).complete(action.getExecuteStop());
                    } else if (action.hasCommunicationCompleted()) {
                        taskId = null;
                        taskCompleted.complete(null);
                    }
                });
            } catch (StatusRuntimeException e) {
                System.err.println("Terminated by " + e);
                e.printStackTrace(System.err);
            } catch (Exception e) {
                System.err.println("Unexpected error: " + e.getMessage());
                e.printStackTrace(System.err);
                assert false;
            }
        }
    }
}
