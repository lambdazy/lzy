package ai.lzy.iam;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.grpc.service.LzyABSService;
import ai.lzy.iam.grpc.service.LzyASService;
import ai.lzy.iam.grpc.service.LzyAuthService;
import ai.lzy.iam.grpc.service.LzySubjectService;
import ai.lzy.iam.storage.impl.DbAuthService;
import ai.lzy.model.grpc.ChannelBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LzyIAM {

    public static final Logger LOG;
    private static final Options options = new Options();

    static {
        options.addOption("p", "port", true, "gRPC port setting");
        options.addOption("l", "limit", true, "user limit setting");
    }

    static {
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyIAM.class);
    }

    private final Server iamServer;

    public LzyIAM(ApplicationContext context) {
        ServiceConfig config = context.getBean(ServiceConfig.class);
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

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-snapshot", options);
            System.exit(-1);
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "8443"));
        final int userLimit = Integer.parseInt(parse.getOptionValue('l', "60"));
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setServerPort(port);
        serviceConfig.setUserLimit(userLimit);
        Map<String, Object> properties = Map.of(
                "serviceConfig", serviceConfig
        );
        try (ApplicationContext context = ApplicationContext.run(properties)) {
            try {
                final LzyIAM lzyIAM = new LzyIAM(context);

                lzyIAM.start();
                lzyIAM.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.info("Shutdown");
            }
        }
    }

    public void start() throws IOException {
        iamServer.start();
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
}
