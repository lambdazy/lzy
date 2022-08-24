package ai.lzy.portal;

import static java.util.Objects.requireNonNull;

import ai.lzy.model.GrpcConverter;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.model.SlotInstance;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsGrpc;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalGrpc;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Operations.SlotStatus;
import ai.lzy.v1.Servant;
import ai.lzy.v1.Tasks;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.Assert;

@SuppressWarnings({"ResultOfMethodCallIgnored", "SameParameterValue"})
class ServerMock extends LzyServerGrpc.LzyServerImplBase {
    final int port;
    final ApplicationContext ctx;
    final Server server;
    final Map<String, ServantHandler> servantHandlers = new ConcurrentHashMap<>();

    ServerMock() {
        this.port = GrpcUtils.rollPort();
        Map<String, Object> properties = Map.of("server.server-uri", "grpc://localhost:" + port);
        this.ctx = ApplicationContext.run(properties);
        this.server = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(this))
            .build();
    }

    int port() {
        return port;
    }

    void start1() throws IOException {
        server.start();
    }

    void stop() throws InterruptedException {
        for (var servant : servantHandlers.values()) {
            servant.shutdown();
            servant.join();
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
                                 StreamObserver<Lzy.GetS3CredentialsResponse> responseObserver) {
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

    void assertPortalNotActive(String servantId) {
        System.out.println("Test portal on '" + servantId + "'...");
        requireNonNull(servantHandlers.get(servantId)).assertPortalNotActive();
    }

    void assertServantNotActive(String servantId) {
        System.out.println("Test servant on '" + servantId + "'...");
        requireNonNull(servantHandlers.get(servantId)).assertServantNotActive();
    }

    void startPortalOn(String servantId) {
        System.out.println("Starting portal on servant '" + servantId + "'...");
        requireNonNull(servantHandlers.get(servantId)).startPortal(servantId + ":stdout", servantId + ":stderr");
    }

    ServantHandler portal() {
        return servantHandlers.values().stream()
            .filter(w -> w.portal)
            .findFirst()
            .orElseThrow();
    }

    ServantHandler servant(String servantId) {
        return servantHandlers.get(servantId);
    }

    void openPortalSlots(LzyPortalApi.OpenSlotsRequest request) {
        var response = portal().portalStub.openSlots(request);
        Assert.assertTrue(response.getDescription(), response.getSuccess());
    }

    String openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest request) {
        var response = portal().portalStub.openSlots(request);
        Assert.assertFalse(response.getSuccess());
        return response.getDescription();
    }

    void waitTaskCompleted(String servantId, String taskId) throws Exception {
        var taskStatus = servant(servantId).tasks.get(taskId).get();
        Assert.assertEquals(taskStatus.getDescription(), 0, taskStatus.getRc());
    }

    void waitPortalCompleted() {
        portal().waitPortalCompleted();
    }

    static class ServantHandler extends Thread {
        private final String servantId;
        private final LzyServantGrpc.LzyServantBlockingStub servantStub;
        private final LzyFsGrpc.LzyFsBlockingStub servantFsStub;
        private final LzyPortalGrpc.LzyPortalBlockingStub portalStub;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private volatile boolean portal = false;
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
            var servantFsUri = req.getFsURI();
            this.servantStub = LzyServantGrpc.newBlockingStub(
                ChannelBuilder.forAddress(servantUri.substring("servant://".length()))
                    .usePlaintext()
                    .enableRetry(LzyServantGrpc.SERVICE_NAME)
                    .build());
            this.servantFsStub = LzyFsGrpc.newBlockingStub(
                ChannelBuilder.forAddress(servantFsUri.substring("fs://".length()))
                    .usePlaintext()
                    .enableRetry(LzyFsGrpc.SERVICE_NAME)
                    .build());
            this.portalStub = LzyPortalGrpc.newBlockingStub(
                ChannelBuilder.forAddress(servantUri.substring("servant://".length()))
                    .usePlaintext()
                    .enableRetry(LzyPortalGrpc.SERVICE_NAME)
                    .build());
        }

        public void shutdown() {
            servantStub.stop(IAM.Empty.getDefaultInstance());
        }

        public void waitStart() throws Exception {
            started.get();
        }

        public void assertPortalNotActive() {
            Assert.assertFalse(portal);
            try {
                portalStub.status(Empty.getDefaultInstance());
                Assert.fail("Portal is active at '" + servantId + "'");
            } catch (StatusRuntimeException e) {
                Assert.assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
            }

            try {
                portalStub.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder().build());
                Assert.fail("Portal is active at '" + servantId + "'");
            } catch (StatusRuntimeException e) {
                Assert.assertEquals(Status.UNIMPLEMENTED.getCode(), e.getStatus().getCode());
            }
        }

        public void assertServantNotActive() {
            Assert.assertTrue(portal);
            try {
                servantStub.execute(Tasks.TaskSpec.getDefaultInstance());
                Assert.fail("Servant is active at '" + servantId + "'");
            } catch (StatusRuntimeException e) {
                Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
            }
        }

        public void startPortal(String stdoutChannel, String stderrChannel) {
            portalStub.start(LzyPortalApi.StartPortalRequest.newBuilder()
                .setStdoutChannelId(stdoutChannel)
                .setStderrChannelId(stderrChannel)
                .build());
            portalStub.status(Empty.getDefaultInstance());
            portal = true;
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

        public void waitPortalCompleted() {
            Assert.assertTrue(portal);

            boolean done = false;
            while (!done) {
                var status = portalStub.status(Empty.getDefaultInstance());
                done = status.getSlotsList().stream().allMatch(
                    slot -> {
                        System.out.println("[portal slot] " + JsonUtils.printSingleLine(slot));
                        return switch (slot.getSlot().getDirection()) {
                            case INPUT -> Set.of(SlotStatus.State.UNBOUND, SlotStatus.State.OPEN,
                                SlotStatus.State.DESTROYED).contains(slot.getState());
                            case OUTPUT -> true;
                            case UNKNOWN, UNRECOGNIZED -> throw new RuntimeException("Unexpected state");
                        };
                    });
                if (!done) {
                    LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
                }
            }
        }

        public Iterator<LzyFsApi.Message> openOutputSlot(SlotInstance slot) {
            return servantFsStub.openOutputSlot(
                LzyFsApi.SlotRequest.newBuilder()
                    .setSlotInstance(GrpcConverter.to(slot))
                    .setOffset(0)
                    .build());
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
            ((ManagedChannel) servantStub.getChannel()).shutdown();
            ((ManagedChannel) servantFsStub.getChannel()).shutdown();
            ((ManagedChannel) portalStub.getChannel()).shutdown();
        }
    }
}
