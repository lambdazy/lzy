package ai.lzy.servant.agents;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.UriScheme;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.model.logs.UserEvent;
import ai.lzy.model.logs.UserEvent.UserEventType;
import ai.lzy.model.logs.UserEventLogger;
import ai.lzy.servant.portal.Portal;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalGrpc;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Servant;
import ai.lzy.v1.Tasks;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LzyServant implements Closeable {

    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final LzyAgent agent;
    private final LzyFsServer lzyFs;
    private final LzyContext context;
    private final AtomicReference<LzyExecution> currentExecution = new AtomicReference<>(null);
    private final Portal portal;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();

    public LzyServant(LzyAgentConfig config)
        throws URISyntaxException, IOException {
        agent = new LzyAgent(config, "LzyServant", new ServantImpl(), new PortalImpl());
        LOG.info("Starting servant at {}://{}:{}/{} with fs at {}:{}",
            UriScheme.LzyServant.scheme(),
            config.getAgentHost(),
            config.getAgentPort(),
            config.getRoot(),
            config.getAgentHost(),
            config.getFsPort());

        long start = System.currentTimeMillis();

        final ManagedChannel channel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(channel);

        long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(
                    "context_id", config.getAgentId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );

        start = System.currentTimeMillis();
        UserEventLogger.log(new UserEvent(
            "Servant startup",
            Map.of(
                "context_id", config.getAgentId(),
                "address", agent.uri().toString()
            ),
            UserEvent.UserEventType.TaskStartUp
        ));
        agent.updateStatus(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(agent.auth());
        commandBuilder.setServantURI(agent.uri().toString());
        commandBuilder.setFsURI(agent.fsUri().toString());
        commandBuilder.setServantId(config.getAgentId());

        /*
         * set status BEFORE actual register call to avoid race between:
         * 1. setting status
         * 2. `start` call from server
         *
         * This solution is valid here because if `register` method fails, servant crashes
         */
        agent.updateStatus(AgentStatus.REGISTERED);
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());

        lzyFs = agent.fs();
        context = agent.context();
        portal = new Portal(config.getAgentId(), lzyFs);

        finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant startUp time",
                Map.of(
                    "context_id", config.getAgentId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
        started.complete(true);
    }

    private void waitForStart() {
        try {
            started.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e);
        }
    }

    private void forceStop(Throwable th) {
        LOG.error("Force terminate servant {}: {}", agent.id(), th);
        try {
            portal.stop();
            cleanupExecution();
            agent.shutdownNow();
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

    public void awaitTermination() throws InterruptedException, IOException {
        agent.awaitTermination();
    }

    @Override
    public void close() throws IOException {
        agent.close();
    }

    private class ServantImpl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void env(Operations.EnvSpec request, StreamObserver<Servant.EnvResult> responseObserver) {
            try {
                if (portal.isActive() || agent.getStatus() != AgentStatus.REGISTERED) {
                    responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                    return;
                }

                LOG.info("Servant::prepare " + JsonUtils.printRequest(request));
                UserEventLogger.log(new UserEvent(
                    "Servant execution preparing",
                    Map.of(
                        "servant_id", agent.id()
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
                                .newBuilder().setAuth(agent.auth()).build()).getBucket();
                            final Lzy.GetS3CredentialsResponse credentials = server.getS3Credentials(
                                Lzy.GetS3CredentialsRequest.newBuilder()
                                    .setBucket(bucket)
                                    .setAuth(agent.auth())
                                    .build()
                            );
                            context.prepare(GrpcConverter.from(request), StorageClient.create(credentials));
                        } catch (EnvironmentInstallationException e) {
                            LOG.error("Unable to install environment", e);
                            result.setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc());
                            result.setDescription(e.getMessage());
                        }
                        responseObserver.onNext(result.build());
                        responseObserver.onCompleted();
                    });
            } catch (Exception e) {
                forceStop(e);
            }
        }

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            waitForStart();
            if (portal.isActive() || agent.getStatus() != AgentStatus.REGISTERED) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }

            context.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.getStatusCase() == Servant.ServantProgress.StatusCase.CONCLUDED) {
                    responseObserver.onCompleted();
                }
            });
            context.start();
        }

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            try {
                if (portal.isActive() || agent.getStatus() != AgentStatus.REGISTERED) {
                    responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                    return;
                }

                if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                    LOG.debug("Servant::execute " + JsonUtils.printRequest(request));
                } else {
                    LOG.info("Servant::execute request (tid={})", request.getTid());
                }
                if (agent.getStatus() == AgentStatus.EXECUTING) {
                    responseObserver.onError(
                        Status.RESOURCE_EXHAUSTED.withDescription("Already executing").asException());
                    return;
                }

                agent.updateStatus(AgentStatus.PREPARING_EXECUTION);
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
                        final String channelName = entry.binding().contains("!")
                                                 ? entry.binding().split("!")[1]
                                                 : entry.binding();
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
                    LOG.info("Servant::progress {} {}", agent.uri(), JsonUtils.printRequest(progress));
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
                        LOG.info("Servant::executionStop {}, ready for the new one", agent.uri());
                        agent.updateStatus(AgentStatus.REGISTERED);
                    }
                });
                currentExecution.set(lzyExecution);
                agent.updateStatus(AgentStatus.EXECUTING);
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
                LOG.info("Servant::stop {}", agent.uri());
                cleanupExecution();
                responseObserver.onNext(IAM.Empty.newBuilder().build());
                responseObserver.onCompleted();

                context.close(); //wait for slots to complete
                UserEventLogger.log(new UserEvent(
                    "Servant task exit",
                    Map.of(
                        "task_id", agent.id(),
                        "address", agent.uri().toString(),
                        "exit_code", String.valueOf(0)
                    ),
                    UserEvent.UserEventType.TaskStop
                ));
                portal.stop();
                agent.shutdown();
            } catch (Exception e) {
                LOG.error("Error during agent server shutdown", e);
            } finally {
                lzyFs.stop();
            }
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<IAM.Empty> responseObserver) {
            if (portal.isActive() || agent.getStatus().getValue() < AgentStatus.EXECUTING.getValue()) {
                responseObserver.onError(Status.FAILED_PRECONDITION.asException());
                return;
            }
            if (agent.getStatus() != AgentStatus.EXECUTING) {
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
            agent.update(server.zygotes(request), responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            agent.status(request, responseObserver);
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
