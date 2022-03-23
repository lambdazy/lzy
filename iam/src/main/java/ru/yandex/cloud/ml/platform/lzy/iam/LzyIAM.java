package ru.yandex.cloud.ml.platform.lzy.iam;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.LzyASService;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.lzy.v1.IAM.Resource;
import yandex.cloud.priv.lzy.v1.IAM.Subject;
import yandex.cloud.priv.lzy.v1.LAS.AuthorizeRequest;
import yandex.cloud.priv.lzy.v1.LzyASGrpc;

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
                LzyASService accessService = context.getBean(LzyASService.class);
//                LzyABSService accessBindingService = context.getBean(LzyABSService.class);
                builder.addService(accessService);
//                builder.addService(accessBindingService);

                final Server server = builder.build();
                server.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("gRPC server is shutting down!");
                    server.shutdown();
                }));

                AuthorizeRequest authorizeRequest = AuthorizeRequest.newBuilder()
                    .setResource(Resource.newBuilder()
                        .setId("res_id")
                        .setType("workflow")
                        .build())
                    .setPermission("lzy.workflow.get")
                    .setSubjectId("atem11")
                    .build();
                LzyASGrpc.LzyASBlockingStub client =
                    LzyASGrpc.newBlockingStub(ChannelBuilder.forAddress("localhost", 8443)
                        .usePlaintext().enableRetry(LzyASGrpc.SERVICE_NAME).build());
                final Subject authorize = client.authorize(authorizeRequest);
                System.out.println(authorize.getId());

                server.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.info("Shutdown");
            }
        }
    }
}
