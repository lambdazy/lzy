package ai.lzy.test.impl;

import ai.lzy.iam.LzyIAM;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAccessServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.test.LzyIAMTestContext;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class IAMThreadContext implements LzyIAMTestContext {
    private static final Logger LOG = LogManager.getLogger(IAMThreadContext.class);

    private static final Duration IAM_STARTUP_TIME = Duration.ofSeconds(10);
    private static final Duration CHANNEL_SHUTDOWN_TIME = Duration.ofSeconds(5);

    public static final int IAM_PORT = 8443;

    private LzyAccessServiceGrpc.LzyAccessServiceBlockingStub lzyAccessServiceClient;
    private LzySubjectServiceGrpc.LzySubjectServiceBlockingStub lzySubjectServiceClient;
    private LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub lzyAccessBindingServiceBlockingStub;
    private LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub lzyAuthenticateServiceBlockingStub;
    private LzyIAM lzyIAM;
    private ManagedChannel channel;
    private ApplicationContext context;

    @Override
    public HostAndPort address() {
        return HostAndPort.fromParts("localhost", IAM_PORT);
    }

    @Override
    public LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessServiceClient() {
        return lzyAccessServiceClient;
    }

    @Override
    public LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectServiceClient() {
        return lzySubjectServiceClient;
    }

    @Override
    public LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub accessBindingServiceClient() {
        return lzyAccessBindingServiceBlockingStub;
    }

    @Override
    public LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub authenticateServiceClient() {
        return lzyAuthenticateServiceBlockingStub;
    }

    @Override
    public void init() {
        var props = Utils.loadModuleTestProperties("iam");

        props.put("iam.server-port", IAM_PORT);

        try {
            LOG.info("Starting LzyIAM on port {}...", IAM_PORT);

            context = ApplicationContext.run(PropertySource.of(props));

            try {
                lzyIAM = new LzyIAM(context);
                lzyIAM.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            LOG.fatal("Failed to start IAM: {}", e.getMessage(), e);
            throw e;
        }

        channel = ChannelBuilder
                .forAddress("localhost", IAM_PORT)
                .usePlaintext()
                .enableRetry(LzySubjectServiceGrpc.SERVICE_NAME)
                .enableRetry(LzyAccessServiceGrpc.SERVICE_NAME)
                .build();
        lzySubjectServiceClient = LzySubjectServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS);
        lzyAccessServiceClient = LzyAccessServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS);
        lzyAccessBindingServiceBlockingStub = LzyAccessBindingServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS);
        lzyAuthenticateServiceBlockingStub = LzyAuthenticateServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS);
        while (channel.getState(true) != ConnectivityState.READY) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(CHANNEL_SHUTDOWN_TIME.getSeconds(), TimeUnit.SECONDS);
            lzyIAM.close();
            lzyIAM.awaitTermination();
            context.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
