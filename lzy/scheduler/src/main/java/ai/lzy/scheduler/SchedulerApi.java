package ai.lzy.scheduler;

import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class SchedulerApi {
    private static final Logger LOG = LogManager.getLogger(SchedulerApi.class);

    public static final String APP = "LzyScheduler";

    private final Server server;
    private final JobService jobService;

    public SchedulerApi(SchedulerGrpc.SchedulerImplBase impl, ServiceConfig config,
                        @Named("SchedulerIamGrpcChannel") ManagedChannel iamChannel, JobService jobService)
    {
        this.jobService = jobService;
        LOG.info("Building server at 0.0.0.0:{}", config.getPort());

        var builder = newGrpcServer("0.0.0.0", config.getPort(),
            new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)));

        var internalOnly = new AccessServerInterceptor(
            new AccessServiceGrpcClient(APP, iamChannel),
            config.getIam().createRenewableToken()::get, Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);

        builder.addService(ServerInterceptors.intercept(impl, internalOnly));
        server = builder.build();

        LOG.info("Starting scheduler on port {}...", config.getPort());

        try {
            server.start();
        } catch (IOException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        LOG.info("Shutdown scheduler...");
        server.shutdown();
        jobService.stop();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) {
        final SchedulerApi api;
        try (var context = ApplicationContext.run()) {
            api = context.getBean(SchedulerApi.class);
            final Thread thread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping GraphExecutor service");
                api.close();
                while (true) {
                    try {
                        thread.join();
                        break;
                    } catch (InterruptedException e) {
                        LOG.debug(e);
                    }
                }
            }));
            while (true) {
                try {
                    api.awaitTermination();
                    break;
                } catch (InterruptedException ignored) {
                    // ignored
                }
            }
        }
    }

}
