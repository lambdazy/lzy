package ai.lzy.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.scheduler.allocator.AllocatorImpl;
import ai.lzy.test.impl.Utils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.AllocatorGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Factory
public class ContextFactory {
    private static final Logger LOG = LogManager.getLogger(ContextFactory.class);

    @Singleton
    ServiceContext<AllocatorGrpc.AllocatorBlockingStub> allocatorService() {
        return new ServiceContext<>(future -> {
            var address = HostAndPort.fromString("localhost:10239");
            final var opts = new HashMap<String, Object>(Map.of(
                    "allocator.address", address.toString(),
                    "allocator.thread-allocator.enabled", "true",
                    "allocator.thread-allocator.vm-jar-file", "../servant/target/servant-1.0-SNAPSHOT.jar",
                    "allocator.thread-allocator.vm-class-name", "ai.lzy.servant.agents.Worker"
            ));
            opts.putAll(Utils.createModuleDatabase("allocator"));

            final var context = ApplicationContext.run(opts);
            final var main = context.getBean(AllocatorMain.class);
            AllocatorImpl.randomServantPorts.set(true);
            try {
                main.start();
            } catch (IOException e) {
                future.completeExceptionally(e);
                return;
            }

            final var channel = ChannelBuilder.forAddress(address)
                .usePlaintext()
                .enableRetry(AllocatorGrpc.SERVICE_NAME)
                .build();

            final var stub = AllocatorGrpc.newBlockingStub(channel);
            future.complete(
                new ServiceContext.Result<>(stub, address)
            );

            try {
                main.awaitTermination();
            } catch (InterruptedException e) {
                try {
                    main.destroyAll();
                } catch (SQLException ex) {
                    LOG.error(ex);
                }
                main.stop();
                channel.shutdown();
                channel.awaitTermination(10, TimeUnit.SECONDS);
            }
        });
    }
}
