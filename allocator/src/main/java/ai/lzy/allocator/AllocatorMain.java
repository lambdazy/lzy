package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.disk.DiskService;
import ai.lzy.allocator.services.AllocatorApi;
import ai.lzy.allocator.services.AllocatorPrivateApi;
import ai.lzy.allocator.vmpool.VmPoolService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.MetricsGrpcInterceptor;
import ai.lzy.v1.AllocatorPrivateGrpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import javax.inject.Named;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class AllocatorMain {
    private static final Logger LOG = LogManager.getLogger(AllocatorMain.class);

    public static final String APP = "LzyAllocator";

    private final ServiceConfig config;
    private final Server server;
    private final GarbageCollector gc;
    private final VmDao vmDao;
    private final VmAllocator alloc;
    private final MetricReporter metricReporter;

    public AllocatorMain(MetricReporter metricReporter, AllocatorApi allocator,
                         AllocatorPrivateApi allocatorPrivate, DiskService diskService,
                         ServiceConfig config, GarbageCollector gc, VmPoolService vmPool,
                         @Named("AllocatorOperationDao") OperationDao operationDao,
                         @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                         VmDao vmDao, VmAllocator alloc)
    {
        this.config = config;
        this.gc = gc;
        this.vmDao = vmDao;
        this.alloc = alloc;
        this.metricReporter = metricReporter;

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

        var opApi = new OperationService(operationDao);

        builder.addService(ServerInterceptors.intercept(allocator, internalOnly));
        builder.addService(allocatorPrivate);
        builder.addService(ServerInterceptors.intercept(opApi, internalOnly));
        builder.addService(ServerInterceptors.intercept(vmPool, internalOnly));
        builder.addService(ServerInterceptors.intercept(diskService, internalOnly));

        this.server = builder.build();
    }

    public void start() throws IOException {
        LOG.info("Starting allocator at {}...", config.getAddress());
        metricReporter.start();
        server.start();
    }

    public void stop() {
        LOG.info("Shutdown allocator at {}...", config.getAddress());
        server.shutdown();
        metricReporter.stop();
        gc.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @VisibleForTesting
    public void destroyAll() throws SQLException {
        LOG.info("Deallocating all vms");
        final var vms = vmDao.listAlive();
        vms.forEach(vm -> {
            // TODO: remove vm subject
            alloc.deallocate(vm.vmId());
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final var context = Micronaut.build(args)
            .banner(true)
            .deduceEnvironment(true)
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
        main.awaitTermination();
    }
}
