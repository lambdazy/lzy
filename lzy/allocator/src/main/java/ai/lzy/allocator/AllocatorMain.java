package ai.lzy.allocator;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.gc.GarbageCollector;
import ai.lzy.allocator.services.AllocatorPrivateService;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.allocator.services.DiskService;
import ai.lzy.allocator.services.VmPoolService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.MetricsGrpcInterceptor;
import ai.lzy.v1.AllocatorPrivateGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class AllocatorMain {
    private static final Logger LOG = LogManager.getLogger(AllocatorMain.class);

    public static final String APP = "LzyAllocator";

    private final ServiceConfig config;
    private final Server server;
    private final GarbageCollector gc;
    private final AllocationContext allocationContext;
    private final MetricReporter metricReporter;

    public AllocatorMain(@Named("AllocatorMetricReporter") MetricReporter metricReporter, AllocatorService allocator,
                         AllocatorPrivateService allocatorPrivate, DiskService diskService,
                         ServiceConfig config, GarbageCollector gc, VmPoolService vmPool,
                         @Named("AllocatorOperationsService") OperationsService operationsService,
                         @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                         AllocationContext allocationContext)
    {
        this.config = config;
        this.gc = gc;
        this.allocationContext = allocationContext;
        this.metricReporter = metricReporter;

        LOG.info("Starting {} with id {}", APP, Objects.requireNonNull(config.getInstanceId()));

        LOG.info("""
                io.netty.eventLoopThreads={}
                io.grpc.netty.shaded.io.netty.eventLoopThreads={}
                availableProcessors={}
                """,
            System.getProperty("io.netty.eventLoopThreads"),
            System.getProperty("io.grpc.netty.shaded.io.netty.eventLoopThreads"),
            Runtime.getRuntime().availableProcessors());

        final HostAndPort address = HostAndPort.fromString(config.getAddress());

        var builder = newGrpcServer("0.0.0.0", address.getPort(),
            new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel))
                .withUnauthenticated(AllocatorPrivateGrpc.getHeartbeatMethod()))
            .intercept(MetricsGrpcInterceptor.server(APP));

        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        builder.addService(ServerInterceptors.intercept(allocator, internalOnly));
        builder.addService(allocatorPrivate);
        builder.addService(ServerInterceptors.intercept(operationsService, internalOnly));
        builder.addService(ServerInterceptors.intercept(vmPool, internalOnly));
        builder.addService(ServerInterceptors.intercept(diskService, internalOnly));

        this.server = builder.build();
    }

    public void start() throws IOException {
        LOG.info("Starting allocator at {}...", config.getAddress());
        metricReporter.start();
        server.start();
        gc.start();
    }

    public void stop() {
        LOG.info("Shutdown allocator at {}...", config.getAddress());
        server.shutdown();
        metricReporter.stop();
        gc.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        LOG.info("Awaiting termination...");
        server.awaitTermination();
    }

    public void destroyAllForTests() throws SQLException {
        LOG.info("Deallocating all vms");
        final var vms = allocationContext.vmDao().listAlive();
        var ops = vms.stream()
            .map(vm -> {
                switch (vm.status()) {
                    case ALLOCATING, DELETING -> { return null; }
                    case RUNNING, IDLE -> { }
                }
                try {
                    return allocationContext.submitDeleteVmAction(vm, "Force clean", "test", LOG);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(Objects::nonNull)
            .toList();

        ops.forEach(opId -> {
            while (true) {
                try {
                    var op = allocationContext.operationsDao().get(opId, null);
                    if (op == null || op.done()) {
                        return;
                    }
                    LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final var context = Micronaut.build(args)
            .banner(true)
            .eagerInitSingletons(true)
            .mainClass(AllocatorMain.class)
            .defaultEnvironments("local")
            .start();

        Properties props = System.getProperties();
        props.setProperty("kubernetes.disable.autoConfig", "true");
        props.setProperty("kubeconfig", "");

        final var main = context.getBean(AllocatorMain.class);
        main.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping allocator service");
            main.stop();
        }));
    }
}
