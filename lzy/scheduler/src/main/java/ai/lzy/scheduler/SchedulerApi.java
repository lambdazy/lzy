package ai.lzy.scheduler;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RemoteAddressInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class SchedulerApi {
    private static final Logger LOG = LogManager.getLogger(SchedulerApi.class);

    public static final String APP = "LzyScheduler";

    private final Server server;
    private final SchedulerApiImpl impl;
    private final WorkerDao dao;

    @Inject
    public SchedulerApi(SchedulerApiImpl impl, PrivateSchedulerApiImpl privateApi, ServiceConfig config,
                        @Named("SchedulerIamGrpcChannel") ManagedChannel iamChannel, WorkerDao dao)
    {
        this.impl = impl;
        this.dao = dao;

        var builder = NettyServerBuilder.forPort(config.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .intercept(new RemoteAddressInterceptor())
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create());

        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        builder.addService(ServerInterceptors.intercept(impl, internalOnly));
        builder.addService(privateApi);
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
        impl.close();
    }

    public void awaitTermination() throws InterruptedException {
        impl.awaitTermination();
        server.awaitTermination();
    }

    @VisibleForTesting
    public void awaitWorkflowTermination(String workflowName) {
        while (true) {
            try {
                if (!(dao.get(workflowName).size() > 0)) break;
            } catch (DaoException e) {
                LOG.error("Cannot execute request to dao", e);
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        final SchedulerApi api;
        try (var context = ApplicationContext.run()) {
            api = context.getBean(SchedulerApi.class);
        }
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
