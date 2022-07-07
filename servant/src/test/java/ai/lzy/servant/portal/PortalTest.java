package ai.lzy.servant.portal;

import ai.lzy.model.JsonUtils;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.model.utils.SessionIdInterceptor;
import ai.lzy.priv.v2.*;
import ai.lzy.priv.v2.Operations.SlotStatus;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static io.grpc.Status.INVALID_ARGUMENT;
import static java.util.Objects.requireNonNull;

public class PortalTest {

    public class ServerMock extends LzyServerGrpc.LzyServerImplBase {
        private final int port;
        private final ApplicationContext ctx;
        private final Server server;
        private final Map<String, ServantHandler> servantHandlers = new ConcurrentHashMap<>();

        private static class DirectChannelInfo {
            final AtomicReference<String> inputSlotServantId = new AtomicReference<>(null);
            final AtomicReference<String> outputSlotServantId = new AtomicReference<>(null);
            final AtomicReference<Servant.SlotAttach> inputSlot = new AtomicReference<>(null);
            final AtomicReference<Servant.SlotAttach> outputSlot = new AtomicReference<>(null);

            boolean isCompleted() {
                return inputSlot.get() != null && outputSlot.get() != null;
            }
        }

        private final Map<String, DirectChannelInfo> directChannels = new ConcurrentHashMap<>(); // channelId -> ...

        public ServerMock() {
            this.port = rollPort();
            Map<String, Object> properties = Map.of("server.server-uri", "grpc://localhost:" + port);
            this.ctx = ApplicationContext.run(properties);
            this.server = NettyServerBuilder.forPort(port)
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
                .addService(ServerInterceptors.intercept(this, new SessionIdInterceptor()))
                .build();
        }

        public int port() {
            return port;
        }

        public void start() throws IOException {
            server.start();
        }

        public void stop() throws InterruptedException {
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

            responseObserver.onNext(Lzy.AttachStatus.getDefaultInstance());
            responseObserver.onCompleted();

            var servantHandler = new ServantHandler(request);
            servantHandlers.put(request.getServantId(), servantHandler);

            servantHandler.start();
        }

        @Override
        public void zygotes(IAM.Auth request, StreamObserver<Operations.ZygoteList> responseObserver) {
            responseObserver.onNext(Operations.ZygoteList.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void channel(Channels.ChannelCommand request, StreamObserver<Channels.ChannelStatus> response) {
            switch (request.getCommandCase()) {
                case CREATE -> {
                    var cmd = request.getCreate();
                    if (!cmd.hasDirect()) {
                        response.onError(INVALID_ARGUMENT.withDescription("Not direct channel").asException());
                        return;
                    }

                    var channelName = request.getChannelName();
                    if (directChannels.putIfAbsent(channelName, new DirectChannelInfo()) != null) {
                        response.onError(Status.ALREADY_EXISTS.asException());
                        return;
                    }

                    response.onNext(Channels.ChannelStatus.newBuilder()
                        .setChannel(Channels.Channel.newBuilder()
                            .setChannelId(channelName)
                            .setContentType(cmd.getContentType())
                            .build())
                        .build());
                    response.onCompleted();
                }

                case DESTROY -> {
                    var channel = directChannels.get(request.getChannelName());
                    if (channel == null) {
                        response.onError(Status.NOT_FOUND.asException());
                        return;
                    }

                    if (channel.inputSlotServantId.get() != null) {
                        var servant = requireNonNull(servantHandlers.get(channel.inputSlotServantId.get()));

                        servant.servantFsStub.destroySlot(LzyFsApi.DestroySlotRequest.newBuilder()
                            .setTaskId(Optional.ofNullable(servant.taskId).orElse(""))
                            .setSlotName(channel.inputSlot.get().getSlot().getName())
                            .build());
                    }

                    if (channel.outputSlotServantId.get() != null) {
                        var servant = requireNonNull(servantHandlers.get(channel.outputSlotServantId.get()));

                        servant.servantFsStub.destroySlot(LzyFsApi.DestroySlotRequest.newBuilder()
                            .setTaskId(Optional.ofNullable(servant.taskId).orElse(""))
                            .setSlotName(channel.outputSlot.get().getSlot().getName())
                            .build());
                    }

                    response.onNext(Channels.ChannelStatus.newBuilder()
                        .setChannel(Channels.Channel.newBuilder()
                            .setChannelId(request.getChannelName())
                            .setContentType(makePlainTextDataScheme())
                            .build())
                        .build());
                    response.onCompleted();
                }
                //case STATE -> {}
                default -> response.onError(INVALID_ARGUMENT.withDescription("Unknown command").asException());
            }
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> response) {
            servant().startTask(request, response);
        }
        // --- SERVER GRPC API  ---

        public void waitServantStart(String servantId) throws Exception {
            requireNonNull(servantHandlers.get(servantId)).waitStart();
        }

        public void assertPortalNotActive(String servantId) {
            System.out.println("Test portal on '" + servantId + "'...");
            requireNonNull(servantHandlers.get(servantId)).assertPortalNotActive();
        }

        public void assertServantNotActive(String servantId) {
            System.out.println("Test servant on '" + servantId + "'...");
            requireNonNull(servantHandlers.get(servantId)).assertServantNotActive();
        }

        public void startPortalOn(String servantId) {
            System.out.println("Starting portal on servant '" + servantId + "'...");
            requireNonNull(servantHandlers.get(servantId)).startPortal();
        }

        public ServantHandler portal() {
            return servantHandlers.values().stream()
                .filter(w -> w.portal)
                .findFirst()
                .orElseThrow();
        }

        public ServantHandler servant() {
            return servantHandlers.values().stream()
                .filter(w -> !w.portal)
                .findFirst()
                .orElseThrow();
        }

        public void openPortalSlots(LzyPortalApi.OpenSlotsRequest request) {
            var response = portal().portalStub.openSlots(request);
            Assert.assertTrue(response.getDescription(), response.getSuccess());
        }

        public void waitTaskCompleted(String taskId) throws Exception {
            var taskStatus = servant().tasks.get(taskId).get();
            Assert.assertEquals(taskStatus.getDescription(), 0, taskStatus.getRc());
        }

        public void waitPortalCompleted() {
            portal().waitPortalCompleted();
        }

        private void attachSlot(String servantId, Servant.SlotAttach attach) {
            String channelName = attach.getChannel();
            if (attach.getSlot().getName().startsWith("/dev/std")) {
                Assert.assertTrue(JsonUtils.printSingleLine(attach), attach.getChannel().isEmpty());

                var uri = URI.create(attach.getUri());
                var taskId = uri.getPath().substring(1, uri.getPath().length() - attach.getSlot().getName().length());
                channelName = taskId + ":" + attach.getSlot().getName().substring("/dev/".length());
            }

            var channel = requireNonNull(directChannels.get(channelName));
            switch (attach.getSlot().getDirection()) {
                case INPUT -> {
                    if (!channel.inputSlot.compareAndSet(null, attach)) {
                        throw new RuntimeException("INPUT slot already set."
                            + " Existing: " + JsonUtils.printSingleLine(channel.inputSlot.get())
                            + ", new: " + JsonUtils.printSingleLine(attach));
                    }
                    channel.inputSlotServantId.set(servantId);
                }
                case OUTPUT -> {
                    if (!channel.outputSlot.compareAndSet(null, attach)) {
                        throw new RuntimeException("OUTPUT slot already set."
                            + " Existing: " + JsonUtils.printSingleLine(channel.outputSlot.get())
                            + ", new: " + JsonUtils.printSingleLine(attach));
                    }
                    channel.outputSlotServantId.set(servantId);
                }
                default -> throw new RuntimeException("zzz");
            }

            if (channel.isCompleted()) {
                var inputServant = requireNonNull(servantHandlers.get(channel.inputSlotServantId.get()));
                var inputSlot = requireNonNull(channel.inputSlot.get());
                var outputSlot = requireNonNull(channel.outputSlot.get());

                System.out.println("Connecting channel '" + channelName + "' slots, input='"
                    + inputSlot.getSlot().getName() + "', output='" + outputSlot.getSlot().getName() + "'...");

                var status = inputServant.servantFsStub.connectSlot(LzyFsApi.ConnectSlotRequest.newBuilder()
                        .setTaskId(Optional.ofNullable(inputServant.taskId).orElse(""))
                        .setSlotName(inputSlot.getSlot().getName())
                        .setSlotUri(outputSlot.getUri())
                        .build());

                if (status.getRc().getCode() != LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS) {
                    throw new RuntimeException("slot connect failed: " + JsonUtils.printSingleLine(status));
                }
            }
        }

        private void detachSlot(String servantId, Servant.SlotDetach detach) {
        }

        class ServantHandler extends Thread {
            private final String servantId;
            private final String servantUri;
            private final String servantFsUri;
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
                this.servantUri = req.getServantURI();
                this.servantFsUri = req.getFsURI();
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

            public void startPortal() {
                portalStub.start(Empty.getDefaultInstance());
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
                                case INPUT ->
                                    Set.of(SlotStatus.State.OPEN, SlotStatus.State.DESTROYED).contains(slot.getState());
                                case OUTPUT -> true;
                                case UNRECOGNIZED -> throw new RuntimeException("Unexpected state");
                            };
                        });
                    if (!done) {
                        LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
                    }
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
                        } else if (action.hasAttach()) {
                            ServerMock.this.attachSlot(servantId, action.getAttach());
                        } else if (action.hasDetach()) {
                            ServerMock.this.detachSlot(servantId, action.getDetach());
                        } else if (action.hasExecuteStop()) {
                            tasks.get(taskId).complete(action.getExecuteStop());
                        } else if (action.hasCommunicationCompleted()) {
                            taskId = null; // all slots are flushed
                            taskCompleted.complete(null);
                        }
                    });
                } catch (StatusRuntimeException e) {
                    System.err.println("Terminated by " + e);
                    e.printStackTrace(System.err);
                }
                ((ManagedChannel) servantStub.getChannel()).shutdown();
                ((ManagedChannel) servantFsStub.getChannel()).shutdown();
                ((ManagedChannel) portalStub.getChannel()).shutdown();
            }
        }
    }

    private ServerMock server;
    private Map<String, LzyServant> servants;

    @Before
    public void before() throws IOException {
        server = new ServerMock();
        server.start();
        servants = new HashMap<>();
    }

    @After
    public void after() throws InterruptedException {
        server.stop();
        for (var servant : servants.values()) {
            servant.close();
        }
        server = null;
        servants = null;
    }

    @Test
    public void activatePortal() throws Exception {
        startServant("servant_1");

        server.waitServantStart("servant_1");
        server.assertPortalNotActive("servant_1");

        server.startPortalOn("servant_1");
        server.assertServantNotActive("servant_1");
    }

    @Test
    public void testSnapshotOnPortal() throws Exception {
        // portal
        startServant("portal");
        server.waitServantStart("portal");
        server.startPortalOn("portal");

        // servant
        startServant("servant");
        server.waitServantStart("servant");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        server.channel(makeCreateDirectChannelCommand("channel_1"), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_1:stdin"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stdin channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_1:stdout"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stdout channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_1:stderr"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stderr channel created: " + JsonUtils.printSingleLine(status))));

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(Operations.Slot.newBuilder()
                    .setName("/portal_slot_1")
                    .setMedia(Operations.Slot.Media.FILE)
                    .setDirection(Operations.Slot.Direction.INPUT)
                    .setContentType(makePlainTextDataScheme())
                    .build())
                .setChannelId("channel_1")
                .setSnapshot(LzyPortalApi.PortalSlotDesc.Snapshot.newBuilder()
                    .setId("snapshot_1")
                    .build())
                .build())
            .build());

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = Operations.Slot.newBuilder()
            .setName("/slot_1")
            .setMedia(Operations.Slot.Media.FILE)
            .setDirection(Operations.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();

        // run task and store result at portal
        server.start(Tasks.TaskSpec.newBuilder()
            .setTid("task_1")
            .setZygote(Operations.Zygote.newBuilder()
                .setName("zygote_1")
                .addSlots(taskOutputSlot)
                .setFuze("echo 'i-am-a-hacker' > /tmp/lzy_servant/slot_1")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_1")
                .setSlot(taskOutputSlot)
                .setBinding("channel:channel_1")
                .build())
            .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        server.waitTaskCompleted("task_1");
        server.waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        server.channel(makeDestroyChannelCommand("channel_1"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy 'channel_1': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_1:stdin"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stdin': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_1:stdout"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stdout': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_1:stderr"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stderr': " + JsonUtils.printSingleLine(status))));

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2 -----------------------------------------\n");


        ///// consumer task  /////

        // create channels for task_2
        server.channel(makeCreateDirectChannelCommand("channel_2"), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_2:stdin"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stdin channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_2:stdout"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stdout channel created: " + JsonUtils.printSingleLine(status))));
        server.channel(makeCreateDirectChannelCommand("task_2:stderr"), SuccessStreamObserver.wrap(
            status -> System.out.println("Stderr channel created: " + JsonUtils.printSingleLine(status))));

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(Operations.Slot.newBuilder()
                    .setName("/slot_2")
                    .setMedia(Operations.Slot.Media.FILE)
                    .setDirection(Operations.Slot.Direction.OUTPUT)
                    .setContentType(makePlainTextDataScheme())
                    .build())
                .setChannelId("channel_2")
                .setSnapshot(LzyPortalApi.PortalSlotDesc.Snapshot.newBuilder()
                    .setId("snapshot_1")
                    .build())
                .build())
            .build());

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();

        var taskInputSlot = Operations.Slot.newBuilder()
            .setName("/slot_2")
            .setMedia(Operations.Slot.Media.FILE)
            .setDirection(Operations.Slot.Direction.INPUT)
            .setContentType(makePlainTextDataScheme())
            .build();

        // run task and load data from portal
        server.start(Tasks.TaskSpec.newBuilder()
            .setTid("task_2")
            .setZygote(Operations.Zygote.newBuilder()
                .setName("zygote_2")
                .addSlots(taskInputSlot)
                .setFuze("/tmp/lzy_servant/sbin/cat /tmp/lzy_servant/slot_2 > " + tmpFile.getAbsolutePath())
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_2")
                .setSlot(taskInputSlot)
                .setBinding("channel:channel_2")
                .build())
            .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // wait
        server.waitTaskCompleted("task_2");
        server.waitPortalCompleted();

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        server.channel(makeDestroyChannelCommand("channel_2"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy 'channel_2': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_2:stdin"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stdin': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_2:stdout"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stdout': " + JsonUtils.printSingleLine(status))));
        server.channel(makeDestroyChannelCommand("task_2:stderr"), SuccessStreamObserver.wrap(
            status -> System.out.println("Destroy '/dev/stderr': " + JsonUtils.printSingleLine(status))));

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----------------------------------------------\n");


        var result = new String(Files.readAllBytes(tmpFile.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result);
    }

    private LzyServant startServant(String servantId) throws URISyntaxException, IOException {
        var servant = new LzyServant(LzyAgentConfig.builder()
                .serverAddress(URI.create("grpc://localhost:" + server.port()))
                .whiteboardAddress(URI.create("grpc://localhost:" + rollPort()))
                .servantId(servantId)
                .token("token_" + servantId)
                .bucket("bucket_" + servantId)
                .agentHost("localhost")
                .agentPort(rollPort())
                .fsPort(rollPort())
                .root(Path.of("/tmp/lzy_" + servantId + "/"))
                .build());
        servant.start();
        servants.put(servantId, servant);
        return servant;
    }


    private static Channels.ChannelCommand makeCreateDirectChannelCommand(String channelName) {
        return Channels.ChannelCommand.newBuilder()
            .setChannelName(channelName)
            .setCreate(Channels.ChannelCreate.newBuilder()
                .setContentType(makePlainTextDataScheme())
                .setDirect(Channels.DirectChannelSpec.getDefaultInstance())
                .build())
            .build();
    }

    private static Channels.ChannelCommand makeDestroyChannelCommand(String channelName) {
        return Channels.ChannelCommand.newBuilder()
            .setChannelName(channelName)
            .setDestroy(Channels.ChannelDestroy.getDefaultInstance())
            .build();
    }

    private static Operations.DataScheme makePlainTextDataScheme() {
        return Operations.DataScheme.newBuilder()
            .setType("text")
            .setSchemeType(Operations.SchemeType.plain)
            .build();
    }

    private static int rollPort() {
        return FreePortFinder.find(10000, 20000);
    }

    private abstract static class SuccessStreamObserver<T> implements StreamObserver<T> {
        @Override
        public void onError(Throwable t) {
            t.printStackTrace(System.err);
            Assert.fail(t.getMessage());
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage) {
            return new SuccessStreamObserver<T>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                }
            };
        }

        public static <T> SuccessStreamObserver<T> wrap(Consumer<T> onMessage, Runnable onFinish) {
            return new SuccessStreamObserver<T>() {
                @Override
                public void onNext(T value) {
                    onMessage.accept(value);
                }

                @Override
                public void onCompleted() {
                    onFinish.run();
                }
            };
        }
    }
}
