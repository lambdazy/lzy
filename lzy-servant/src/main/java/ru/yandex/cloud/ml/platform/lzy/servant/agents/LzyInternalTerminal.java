package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyTerminal;

public class LzyInternalTerminal extends LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyInternalTerminal.class);

    private final ManagedChannel serverChannel;
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final URI agentUri;
    private final Server agentServer;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();
    private LzyContext context;

    public LzyInternalTerminal(LzyAgentConfig config) throws URISyntaxException, IOException {
        super(LzyAgentConfig.updateServantId(config, "iterm_" + config.getUser()));

        serverChannel = ChannelBuilder
                .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
                .usePlaintext()
                .enableRetry(LzyServerGrpc.SERVICE_NAME)
                .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        agentUri = new URI(LzyTerminal.scheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        agentServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getAgentHost(), config.getAgentPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new Impl())
            .build();
    }

    @Override
    protected LzyContext context() {
        if (context == null) {
            throw new RuntimeException("Context is not yet defined (who knows what that mean!)");
        }
        return context;
    }

    @Override
    protected URI serverUri() {
        return agentUri;
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
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentUri.toString());
        commandBuilder.setFsURI(lzyFs.getUri().toString());
        commandBuilder.setServantId(config.getServantId());
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);

        context = new LzyContext(config.getServantId(), lzyFs.getSlotsManager(), lzyFs.getMountPoint().toString());

        Context.current().addListener(context -> {
            LOG.info("LzyInternalTerminal session terminated from server ");
            close();
        }, Runnable::run);
    }

    @Override
    protected LzyServerGrpc.LzyServerBlockingStub serverApi() {
        return server;
    }

    @Override
    public void close() {
        LOG.info("Close internal terminal...");
        context.slots().forEach(slot -> {
            LOG.info("  suspending slot {} ({})...", slot.name(), slot.status().getState());
            slot.suspend();
        });
        context.close();
        serverChannel.shutdown();
        agentServer.shutdown();
        super.close();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        serverChannel.awaitTermination(10, TimeUnit.SECONDS);
        super.awaitTermination();
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            waitForStart();
            LzyInternalTerminal.this.context.onProgress(progress -> {
                LOG.info("LzyInternalTerminal::progress {} {}", agentUri, JsonUtils.printRequest(progress));

                responseObserver.onNext(progress);
                if (progress.getStatusCase() == Servant.ServantProgress.StatusCase.EXIT) {
                    responseObserver.onCompleted();
                }
            });

            status.set(AgentStatus.PREPARING_EXECUTION);
            LzyInternalTerminal.this.context.start();
            status.set(AgentStatus.EXECUTING);
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
            LzyInternalTerminal.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyInternalTerminal.this.status(request, responseObserver);
        }
    }
}
