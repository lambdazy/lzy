package ai.lzy.scheduler;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.grpc.RemoteAddressInterceptor;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Singleton
public class SchedulerApi {
    private static final Logger LOG = LogManager.getLogger(SchedulerApi.class);
    private final Server server;
    private final SchedulerApiImpl impl;

    @Inject
    public SchedulerApi(SchedulerApiImpl impl, PrivateSchedulerApiImpl privateApi, ServiceConfig config) {
        this.impl = impl;

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
                .intercept(new RemoteAddressInterceptor())
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        builder.addService(ServerInterceptors.intercept(impl, new GrpcLogsInterceptor()));
        builder.addService(ServerInterceptors.intercept(privateApi, new GrpcLogsInterceptor()));
        server = builder.build();

        LOG.info("Starting scheduler on port {}...", config.port());

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
