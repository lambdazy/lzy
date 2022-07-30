package ai.lzy.servant.agents;

import ai.lzy.model.JsonUtils;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Servant;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LzyInternalTerminal implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyInternalTerminal.class);

    private final ManagedChannel serverChannel;
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final LzyAgent agent;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();

    public LzyInternalTerminal(LzyAgentConfig config)
        throws URISyntaxException, IOException {
        agent = new LzyAgent(LzyAgentConfig.updateAgentId(config, "iterm_" + config.getUser()),
            "LzyInternalTerminal", new Impl());

        LOG.info("Starting InternalTerminal at {}://{}:{}/{} with fs at {}:{}",
            UriScheme.LzyTerminal.scheme(),
            config.getAgentHost(),
            config.getAgentPort(),
            config.getRoot(),
            config.getAgentHost(),
            config.getFsPort());

        serverChannel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        agent.updateStatus(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(agent.auth());
        commandBuilder.setServantURI(agent.uri().toString());
        commandBuilder.setFsURI(agent.fsUri().toString());
        commandBuilder.setServantId(config.getAgentId());
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        agent.updateStatus(AgentStatus.REGISTERED);

        Context.current().addListener(context -> {
            LOG.info("LzyInternalTerminal session terminated from server ");
            close();
        }, Runnable::run);
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
    public void close() {
        LOG.info("Close internal terminal...");
        agent.context().slots().forEach(slot -> {
            LOG.info("  suspending slot {} ({})...", slot.name(), slot.status().getState());
            slot.suspend();
        });
        agent.context().close();
        serverChannel.shutdown();
    }

    public void awaitTermination() throws InterruptedException, IOException {
        serverChannel.awaitTermination(10, TimeUnit.SECONDS);
        agent.awaitTermination();
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            waitForStart();
            agent.context().onProgress(progress -> {
                LOG.info("LzyInternalTerminal::progress {} {}", agent.uri(), JsonUtils.printRequest(progress));
                responseObserver.onNext(progress);
            });

            agent.updateStatus(AgentStatus.PREPARING_EXECUTION);
            agent.context().start();
            agent.updateStatus(AgentStatus.EXECUTING);
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
            agent.update(server.zygotes(request), responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            agent.status(request, responseObserver);
        }
    }
}
