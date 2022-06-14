package ru.yandex.cloud.ml.platform.lzy.gateway;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ru.yandex.cloud.ml.platform.lzy.gateway.configs.GatewayServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.WorkflowService;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App {
    public static final Logger LOG;

    static {
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(App.class);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            try {
                var gatewayServiceConfig = context.getBean(GatewayServiceConfig.class);
                LOG.info("Starting on {}", gatewayServiceConfig.getPort());

                ServerBuilder<?> builder = NettyServerBuilder.forPort(gatewayServiceConfig.getPort())
                        .permitKeepAliveWithoutCalls(true)
                        .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

                builder.intercept(new AuthServerInterceptor(
                        new AuthenticateServiceGrpcClient(GrpcConfig.from(gatewayServiceConfig.getIamAddress()))));
                builder.addService(context.getBean(WorkflowService.class));

                final Server server = builder.build();
                server.start();

                SignalHandler signalHandler = sig -> {
                    System.out.println("gRPC server is shutting down by signal " + sig);
                    server.shutdown();
                };

                Signal.handle(new Signal("INT"), signalHandler);
                Signal.handle(new Signal("TERM"), signalHandler);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("gRPC server is shutting down!");
                    server.shutdown();
                }));

                server.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.error(e);
            }
        }
    }
}
