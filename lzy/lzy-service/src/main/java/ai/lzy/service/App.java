package ai.lzy.service;

import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AllowSubjectOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.gc.GarbageCollector;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.service.util.ExecutionIdInterceptor;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ai.lzy.service.LzyService.APP;

@Singleton
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    private final Server grpcServer;
    private final LzyServiceConfig config;
    private final MetricReporter metricReporter;
    private final GarbageCollector garbageCollector;

    public App(LzyServiceConfig config, LzyService lzyService, LzyPrivateService lzyPrivateService,
               @Named("IamServiceChannel") ManagedChannel iamChannel,
               @Named("LzyServiceAuthInterceptor") AuthServerInterceptor authInterceptor,
               @Named("LzyServiceMetricReporter") MetricReporter metricReporter,
               ClientVersionInterceptor clientVersionInterceptor,
               GarbageCollector garbageCollector)
    {
        this.config = config;
        this.metricReporter = metricReporter;
        this.garbageCollector = garbageCollector;
        final var internalOnly = new AccessServerInterceptor(
            new AccessServiceGrpcClient(APP, iamChannel),
            config.getIam().createRenewableToken()::get, Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);
        this.grpcServer = createServer(
            HostAndPort.fromString(config.getAddress()),
            clientVersionInterceptor,
            authInterceptor,
            ServerInterceptors.intercept(lzyService, new ExecutionIdInterceptor()),
            ServerInterceptors.intercept(lzyPrivateService, internalOnly));
    }

    public void start() throws IOException {
        grpcServer.start();
        metricReporter.start();
        LOG.info("LzyServer started at {}",
            grpcServer.getListenSockets().stream().map(Object::toString).collect(Collectors.joining(", ")));
        if (config.getGc().isEnabled()) {
            garbageCollector.start();
        }
    }

    public void shutdown(boolean force) {
        garbageCollector.shutdown();
        metricReporter.stop();
        if (force) {
            grpcServer.shutdownNow();
        } else {
            grpcServer.shutdown();
        }
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public static Server createServer(HostAndPort endpoint, ClientVersionInterceptor versionInterceptor,
                                      AuthServerInterceptor authInterceptor, ServerServiceDefinition... services)
    {
        var serverBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(500, TimeUnit.MILLISECONDS)
            .keepAliveTime(1000, TimeUnit.MILLISECONDS)
            .keepAliveTimeout(500, TimeUnit.MILLISECONDS)
            .intercept(versionInterceptor)
            .intercept(AllowSubjectOnlyInterceptor.ALLOW_USER_ONLY)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.generate())
            .intercept(GrpcHeadersServerInterceptor.create());

        for (var service : services) {
            serverBuilder.addService(service);
        }

        return serverBuilder.build();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(App.class, args)) {
            var main = context.getBean(App.class);
            main.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping lzy service");
                main.shutdown(false);
            }));
            main.awaitTermination();
        }
    }
}
