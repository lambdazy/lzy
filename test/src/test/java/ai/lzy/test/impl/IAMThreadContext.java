package ai.lzy.test.impl;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v1.LzyAccessServiceGrpc;
import ai.lzy.priv.v1.LzySubjectServiceGrpc;
import ai.lzy.test.LzyIAMTestContext;
import ai.lzy.v1.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.LzyAuthenticateServiceGrpc;
import ai.lzy.whiteboard.api.SnapshotApi;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.testcontainers.shaded.com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class IAMThreadContext implements LzyIAMTestContext {

    private static final long IAM_STARTUP_SECONDS = 60;
    private static final int IAM_PORT = 8443;
    private static final int USER_LIMIT = 60;

    private LzyAccessServiceGrpc.LzyAccessServiceBlockingStub lzyAccessServiceClient;
    private LzySubjectServiceGrpc.LzySubjectServiceBlockingStub lzySubjectServiceClient;
    private LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub lzyAccessBindingServiceBlockingStub;
    private LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub lzyAuthenticateServiceBlockingStub;
    private LzyIAM lzyIAM;
    private ManagedChannel channel;

    @Override
    public HostAndPort address() {
        return HostAndPort.fromString("http://localhost:" + IAM_PORT);
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
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setServerPort(IAM_PORT);
        serviceConfig.setUserLimit(USER_LIMIT);
        Map<String, Object> appProperties = Map.of(
                "serviceConfig", serviceConfig
        );
        try (ApplicationContext context = ApplicationContext.run(appProperties)) {
            var logger = LogManager.getLogger(SnapshotApi.class);
            logger.info("Starting LzyIAM on port {}...", IAM_PORT);

            try {
                lzyIAM = new LzyIAM(context);
                lzyIAM.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        channel = ChannelBuilder
                .forAddress("localhost", IAM_PORT)
                .usePlaintext()
                .enableRetry(LzySubjectServiceGrpc.SERVICE_NAME)
                .enableRetry(LzyAccessServiceGrpc.SERVICE_NAME)
                .build();
        lzySubjectServiceClient = LzySubjectServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_SECONDS, TimeUnit.SECONDS);
        lzyAccessServiceClient = LzyAccessServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_SECONDS, TimeUnit.SECONDS);
        lzyAccessBindingServiceBlockingStub = LzyAccessBindingServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_SECONDS, TimeUnit.SECONDS);
        lzyAuthenticateServiceBlockingStub = LzyAuthenticateServiceGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(IAM_STARTUP_SECONDS, TimeUnit.SECONDS);
        while (channel.getState(true) != ConnectivityState.READY) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            lzyIAM.close();
            lzyIAM.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
