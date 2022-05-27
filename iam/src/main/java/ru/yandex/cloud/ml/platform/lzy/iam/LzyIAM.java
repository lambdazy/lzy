package ru.yandex.cloud.ml.platform.lzy.iam;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.LzyABSService;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.LzyASService;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors.AuthInterceptor;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.impl.DbAuthService;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LzyIAM {

    public static final Logger LOG;

    static {
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyIAM.class);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            try {
                ServerBuilder<?> builder = NettyServerBuilder.forPort(8443)
                        .permitKeepAliveWithoutCalls(true)
                        .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);
                AuthInterceptor authInterceptor =
                        new AuthInterceptor(context.getBean(DbAuthService.class));
                LzyASService accessService = context.getBean(LzyASService.class);
                LzyABSService accessBindingService = context.getBean(LzyABSService.class);
                builder.intercept(authInterceptor);
                builder.addService(accessService);
                builder.addService(accessBindingService);

                final Server server = builder.build();
                server.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("gRPC server is shutting down!");
                    server.shutdown();
                }));

                server.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.info("Shutdown");
            }
        }
    }
}
