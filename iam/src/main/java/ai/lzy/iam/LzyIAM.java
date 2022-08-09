package ai.lzy.iam;

import ai.lzy.iam.configs.DbConfig;
import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.grpc.service.LzyABSService;
import ai.lzy.iam.grpc.service.LzyASService;
import ai.lzy.iam.grpc.service.LzyAuthService;
import ai.lzy.iam.grpc.service.LzySubjectService;
import ai.lzy.iam.storage.db.InternalUserInserter;
import ai.lzy.iam.storage.impl.DbAuthService;
import ai.lzy.util.ChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LzyIAM {

    public static final Logger LOG;

    static {
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyIAM.class);
    }

    private final Server iamServer;

    public LzyIAM(ApplicationContext context) {
        ServiceConfig config = context.getBean(ServiceConfig.class);
        DbConfig dbConfig = context.getBean(DbConfig.class);

        InternalUserConfig internalUserConfig = context.getBean(InternalUserConfig.class);
        InternalUserInserter internalUserInserter = context.getBean(InternalUserInserter.class);
        internalUserInserter.addOrUpdateInternalUser(internalUserConfig);

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.getServerPort())
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);
        AuthServerInterceptor internalAuthInterceptor =
                new AuthServerInterceptor(context.getBean(DbAuthService.class));
        LzyASService accessService = context.getBean(LzyASService.class);
        LzyABSService accessBindingService = context.getBean(LzyABSService.class);
        LzySubjectService subjectService = context.getBean(LzySubjectService.class);
        LzyAuthService authService = context.getBean(LzyAuthService.class);
        builder.intercept(internalAuthInterceptor);
        builder.addService(accessService);
        builder.addService(accessBindingService);
        builder.addService(subjectService);
        builder.addService(authService);

        this.iamServer = builder.build();
    }

    public void start() throws IOException {
        iamServer.start();

        LOG.info("IAM started on {}",
            iamServer.getListenSockets().stream().map(Object::toString).collect(Collectors.joining()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            iamServer.shutdown();
        }));
    }

    public void close() {
        iamServer.shutdownNow();
    }

    public void awaitTermination() throws InterruptedException {
        iamServer.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            try {
                final LzyIAM lzyIAM = new LzyIAM(context);

                lzyIAM.start();
                lzyIAM.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.error("Shutdown, ", e);
                System.exit(-1);
            }
        }
    }
}
