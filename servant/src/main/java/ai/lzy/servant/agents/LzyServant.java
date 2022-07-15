package ai.lzy.servant.agents;

import ai.lzy.model.logs.UserEvent.UserEventType;
import ai.lzy.servant.portal.Portal;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.model.logs.UserEvent;
import ai.lzy.model.logs.UserEventLogger;
import ai.lzy.priv.v2.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.model.UriScheme.LzyServant;

public class LzyServant extends LzyAgent {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final URI agentAddress;
    private final Server agentServer;
    private final AtomicReference<LzyExecution> currentExecution = new AtomicReference<>(null);
    private final Portal portal;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();
    private LzyContext context;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException, IOException {
        super(config);
        final long start = System.currentTimeMillis();

        final ManagedChannel channel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(channel);

        portal = new Portal(config.getServantId(), lzyFs);

        agentAddress =
            new URI(LzyServant.scheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        agentServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(agentAddress.getHost(), agentAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.SECONDS)
            .keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.SECONDS)
            .keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS, TimeUnit.MINUTES)
            .addService(new ServantImpl())
            .addService(new PortalImpl())
            .build();

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(
                    "context_id", config.getServantId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected LzyContext context() {
        return context;
    }

    @Override
    public URI serverUri() {
        return agentAddress;
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    protected void started() {
        started.complete(true);
    }

    private void waitForStart() {
        try {
            started.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e);
        }
    }

    @Override
    protected void onStartUp() {
        final long start = System.currentTimeMillis();
        UserEventLogger.log(new UserEvent(
            "Servant startup",
            Map.of(
                "context_id", config.getServantId(),
                "address", agentAddress.toString()
            ),
            UserEvent.UserEventType.TaskStartUp
        ));
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentAddress.toString());
        commandBuilder.setFsURI(lzyFs.getUri().toString());
        commandBuilder.setServantId(config.getServantId());
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);

        context = new LzyContext(config.getServantId(), lzyFs.getSlotsManager(), lzyFs.getMountPoint().toString());

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant startUp time",
                Map.of(
                    "context_id", config.getServantId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected LzyServerGrpc.LzyServerBlockingStub serverApi() {
        return server;
    }

    private void forceStop(Throwable th) {
        LOG.error("Force terminate servant {}: {}", config.getServantId(), th);
        try {
            portal.stop();
            cleanupExecution();
            agentServer.shutdownNow();
        } finally {
            lzyFs.stop();
        }
    }

    private void cleanupExecution() {
        LOG.info("Cleanup execution");
        var lzyExecution = currentExecution.get();
        if (lzyExecution != null) {
            lzyExecution.signal(Signal.KILL.sig());
        }
    }

    private class ServantImpl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void env(Operations.EnvSpec request, StreamObserver<Servant.EnvResult> responseObserver) {
            if (portal.isActive() || status.get().getValue() != AgentStatus.REGISTERED.getValue()) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            LOG.info("Servant::prepare " + JsonUtils.printRequest(request));
            UserEventLogger.log(new UserEvent(
                "Servant execution preparing",
                Map.of(
                    "servant_id", config.getServantId()
                ),
                UserEvent.UserEventType.ExecutionPreparing
            ));
            // TODO (lindvv): logs without lambda
            MetricEventLogger.timeIt(
                "time of context preparing",
                Map.of("metric_type", "system_metric"),
                () -> {
                    final Servant.EnvResult.Builder result = Servant.EnvResult.newBuilder();
                    try {
                        final String bucket = server.getBucket(Lzy.GetBucketRequest
                            .newBuilder().setAuth(auth).build()).getBucket();
                        final Lzy.GetS3CredentialsResponse credentials = server.getS3Credentials(
                            Lzy.GetS3CredentialsRequest.newBuilder()
                                .setBucket(bucket)
                                .setAuth(auth)
                                .build()
                        );
                        context().prepare(GrpcConverter.from(request), StorageClient.create(credentials));
                    } catch (EnvironmentInstallationException e) {
                        LOG.error("Unable to install environment", e);
                        result.setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc());
                        result.setDescription(e.getMessage());
                    }
                    responseObserver.onNext(result.build());
                    responseObserver.onCompleted();
                });
        }

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            if (portal.isActive() || status.get().getValue() != AgentStatus.REGISTERED.getValue()) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            waitForStart();
            LzyServant.this.context.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.getStatusCase() == Servant.ServantProgress.StatusCase.CONCLUDED) {
                    responseObserver.onCompleted();
                }
            });
            LzyServant.this.context.start();
        }

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            try {
                if (portal.isActive() || status.get().getValue() != AgentStatus.REGISTERED.getValue()) {
                    responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                    return;
                }

                if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                    LOG.debug("Servant::execute " + JsonUtils.printRequest(request));
                } else {
                    LOG.info("Servant::execute request (tid={})", request.getTid());
                }
                if (status.get() == AgentStatus.EXECUTING) {
                    responseObserver.onError(
                        Status.RESOURCE_EXHAUSTED.withDescription("Already executing").asException());
                    return;
                }

                status.set(AgentStatus.PREPARING_EXECUTION);
                final String tid = request.getTid();
                final AtomicZygote zygote = (AtomicZygote) GrpcConverter.from(request.getZygote());
                UserEventLogger.log(new UserEvent(
                    "Servant execution preparing",
                    Map.of(
                        "task_id", tid,
                        "zygote_description", zygote.description()
                    ),
                    UserEvent.UserEventType.ExecutionPreparing
                ));

                GrpcConverter.from(request.getAssignmentsList().stream()).map(
                    entry -> {
                        LzySlot slot = context.getOrCreateSlot(tid, entry.slot(), entry.binding());
                        // TODO: It will be removed after creating Portal
                        final String channelName;
                        if (entry.binding().startsWith("channel:")) {
                            channelName = entry.binding().substring("channel:".length());
                        } else {
                            channelName = entry.binding();
                        }
                        if (channelName.startsWith("snapshot://") && slot instanceof LzyOutputSlot) {
                            final URI channelUri = URI.create(channelName);
                            String snapshotId = "snapshot://" + channelUri.getHost();
                            lzyFs.getSlotConnectionManager().snapshooter()
                                .registerSlot(slot, snapshotId, channelName);
                        }
                        return slot;
                    }
                ).forEach(slot -> {
                    if (slot instanceof LzyFileSlot) {
                        lzyFs.addSlot((LzyFileSlot) slot);
                    }
                });

                final long start = System.currentTimeMillis();
                final LzyExecution lzyExecution = context.execute(tid, zygote, progress -> {
                    LOG.info("Servant::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
                    UserEventLogger.log(new UserEvent(
                        "Servant execution progress",
                        Map.of(
                            "task_id", tid,
                            "zygote_description", zygote.description(),
                            "progress", JsonUtils.printRequest(progress)
                        ),
                        UserEventType.ExecutionProgress
                    ));
                    if (progress.hasExecuteStop()) {
                        UserEventLogger.log(new UserEvent(
                            "Servant execution exit",
                            Map.of(
                                "task_id", tid,
                                "zygote_description", zygote.description(),
                                "exit_code", String.valueOf(progress.getExecuteStop().getRc())
                            ),
                            UserEventType.ExecutionComplete
                        ));
                        LOG.info("Servant::executionStop {}, ready for the new one", agentAddress);
                        status.set(AgentStatus.REGISTERED);
                    }
                });
                currentExecution.set(lzyExecution);
                status.set(AgentStatus.EXECUTING);
                responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
                responseObserver.onCompleted();

                lzyExecution.waitFor();
                final long executed = System.currentTimeMillis();
                MetricEventLogger.log(new MetricEvent(
                    "time of task executing",
                    Map.of("metric_type", "system_metric"),
                    executed - start)
                );
            } catch (Exception e) {
                forceStop(e);
            }
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            try {
                LOG.info("Servant::stop {}", agentAddress);
                cleanupExecution();
                responseObserver.onNext(IAM.Empty.newBuilder().build());
                responseObserver.onCompleted();

                context.close(); //wait for slots to complete
                UserEventLogger.log(new UserEvent(
                    "Servant task exit",
                    Map.of(
                        "task_id", config.getServantId(),
                        "address", agentAddress.toString(),
                        "exit_code", String.valueOf(0)
                    ),
                    UserEvent.UserEventType.TaskStop
                ));
                portal.stop();
                agentServer.shutdown();
            } catch (Exception e) {
                LOG.error("Error during agent server shutdown", e);
            } finally {
                lzyFs.stop();
            }
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<IAM.Empty> responseObserver) {
            if (portal.isActive() || status.get().getValue() < AgentStatus.EXECUTING.getValue()) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (status.get() != AgentStatus.EXECUTING) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Cannot send signal when servant not executing").asException());
                return;
            }
            var lzyExecution = currentExecution.get();
            if (lzyExecution != null) {
                lzyExecution.signal(request.getSigValue());
            }
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
            if (portal.isActive()) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            LzyServant.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyServant.this.status(request, responseObserver);
        }
    }

    private class PortalImpl extends LzyPortalGrpc.LzyPortalImplBase {
        @Override
        public void start(LzyPortalApi.StartPortalRequest request,
            StreamObserver<LzyPortalApi.StartPortalResponse> responseObserver) {
            if (currentExecution.get() != null) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (portal.start(request)) {
                responseObserver.onNext(LzyPortalApi.StartPortalResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.ALREADY_EXISTS.asException());
            }
        }

        @Override
        public void stop(Empty request, StreamObserver<Empty> responseObserver) {
            if (currentExecution.get() != null) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (portal.stop()) {
                portal.stop();
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        @Override
        public void status(Empty request, StreamObserver<LzyPortalApi.PortalStatus> responseObserver) {
            if (currentExecution.get() != null) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (portal.isActive()) {
                var response = portal.status();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        @Override
        public void openSlots(LzyPortalApi.OpenSlotsRequest request,
                              StreamObserver<LzyPortalApi.OpenSlotsResponse> responseObserver) {
            if (currentExecution.get() != null) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (portal.isActive()) {
                try {
                    var response = portal.openSlots(request);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    responseObserver.onError(e);
                }
            } else {
                responseObserver.onError(Status.UNIMPLEMENTED.asException());
            }
        }
    }
}
