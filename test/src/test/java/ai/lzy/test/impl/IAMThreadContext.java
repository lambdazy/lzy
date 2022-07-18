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
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.core.io.scan.DefaultClassPathResourceLoader;
import org.apache.logging.log4j.LogManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class IAMThreadContext implements LzyIAMTestContext {

    private static final Duration IAM_STARTUP_TIME = Duration.ofSeconds(10);
    private static final Duration CHANNEL_SHUTDOWN_TIME = Duration.ofSeconds(5);
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
        final Map<String, Object> props;
        try {
            props = new YamlPropertySourceLoader()
                .read("iam", new FileInputStream("../iam/src/main/resources/application-test.yml"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        props.put("iam.server-port", IAM_PORT);
        props.put("iam.user-limit", USER_LIMIT);

        try (ApplicationContext context = ApplicationContext.run(PropertySource.of(props))) {
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
