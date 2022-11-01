package ai.lzy.iam;

import ai.lzy.iam.configs.InternalUserConfig;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.grpc.service.LzyABSService;
import ai.lzy.iam.grpc.service.LzyASService;
import ai.lzy.iam.grpc.service.LzyAuthService;
import ai.lzy.iam.grpc.service.LzySubjectService;
import ai.lzy.iam.storage.db.InternalUserInserter;
import ai.lzy.iam.storage.impl.DbAuthService;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.IOException;
import java.util.stream.Collectors;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

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

        InternalUserConfig internalUserConfig = context.getBean(InternalUserConfig.class);
        InternalUserInserter internalUserInserter = context.getBean(InternalUserInserter.class);
        internalUserInserter.addOrUpdateInternalUser(internalUserConfig);

        var builder = newGrpcServer("0.0.0.0", config.getServerPort(),
            new AuthServerInterceptor(context.getBean(DbAuthService.class)));

        LzyASService accessService = context.getBean(LzyASService.class);
        LzyABSService accessBindingService = context.getBean(LzyABSService.class);
        LzySubjectService subjectService = context.getBean(LzySubjectService.class);
        LzyAuthService authService = context.getBean(LzyAuthService.class);

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

    public int port() {
        return this.iamServer.getPort();
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
