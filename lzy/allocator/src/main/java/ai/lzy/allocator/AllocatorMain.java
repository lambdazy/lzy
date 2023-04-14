package ai.lzy.allocator;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.alloc.dao.VmDao;
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
import sun.misc.Signal;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public AllocatorMain(@Named("AllocatorMetricReporter") MetricReporter metricReporter, AllocatorService allocator,
                         AllocatorPrivateService allocatorPrivate, DiskService diskService,
                         ServiceConfig config, GarbageCollector gc, VmPoolService vmPool, VmDao vmDao,
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

        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel))
            .withUnauthenticated(
                AllocatorPrivateGrpc.getHeartbeatMethod(),
                AllocatorPrivateGrpc.getRegisterMethod());

        var builder = newGrpcServer("0.0.0.0", address.getPort(), auth)
            .intercept(MetricsGrpcInterceptor.server(APP));

        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);
        var vmOttAuth = new VmOttAuthInterceptor(vmDao, AllocatorPrivateGrpc.getRegisterMethod());

        builder.addService(ServerInterceptors.intercept(allocator, internalOnly));
        builder.addService(ServerInterceptors.intercept(allocatorPrivate, vmOttAuth));
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

    public void stop(boolean graceful) {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }

        LOG.info("{}hutdown allocator at {}...", graceful ? "Graceful s" : "S", config.getAddress());
        server.shutdown();
        gc.shutdown();

        if (graceful) {
            try {
                if (!server.awaitTermination(20, TimeUnit.SECONDS)) {
                    LOG.error("GRPC Server was not terminated in 10 minutes. Force stop.");
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("GRPC Server shutdown was terminated: {}", e.getMessage(), e);
            }
            LOG.info("GRPC server terminated");

            allocationContext.executor().shutdown(Duration.ofMinutes(5));
        }

        metricReporter.stop();
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
                    return allocationContext.startDeleteVmAction(vm, "Force clean", "test", LOG);
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
        System.out.println("Current path is:: " + System.getProperty("user.dir"));

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

        Signal.handle(new Signal("TERM"), sig -> {
            main.stop(true);
            System.exit(0);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> main.stop(false), "main-shutdown-hook"));

        main.awaitTermination();
    }
}
