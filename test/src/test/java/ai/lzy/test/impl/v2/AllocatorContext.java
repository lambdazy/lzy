package ai.lzy.test.impl.v2;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.scheduler.allocator.AllocatorImpl;
import ai.lzy.test.impl.Utils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.AllocatorGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class AllocatorContext {
    private final HostAndPort address;
    private final ApplicationContext context;
    private final AllocatorMain main;
    private final ManagedChannel channel;
    private final AllocatorGrpc.AllocatorBlockingStub stub;

    public AllocatorContext(IamContext iam, String jarPath, String executableClass, int port) {

        this.address = HostAndPort.fromParts("localhost", port);
        final var opts = Utils.createModuleDatabase("allocator");
        opts.putAll(new HashMap<String, Object>(Map.of(
            "allocator.iam.address", iam.address(),
            "allocator.address", address.toString(),
            "allocator.kuber-allocator.enabled", "false",
            "allocator.thread-allocator.enabled", "true",
            "allocator.thread-allocator.vm-jar-file", jarPath,
            "allocator.thread-allocator.vm-class-name", executableClass
        )));

        this.context = ApplicationContext.run(opts);
        this.main = context.getBean(AllocatorMain.class);
        AllocatorImpl.randomServantPorts.set(true);
        try {
            main.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.channel = ChannelBuilder.forAddress(address)
            .usePlaintext()
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .build();

        this.stub = AllocatorGrpc.newBlockingStub(channel);
    }

    public void close() {
        try {
            main.destroyAll();
            main.stop();
            main.awaitTermination();

            channel.shutdown();
            channel.awaitTermination(10, TimeUnit.SECONDS);

            context.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HostAndPort address() {
        return address;
    }

    public AllocatorGrpc.AllocatorBlockingStub stub() {
        return stub;
    }


    @Singleton
    public static class WorkerAllocatorContext extends AllocatorContext {

        @Inject
        public WorkerAllocatorContext(IamContext iam) {
            super(iam, "../servant/target/servant-1.0-SNAPSHOT.jar", "ai.lzy.servant.agents.Worker", 23910);
        }

        @Override
        @PreDestroy
        public void close() {
            super.close();
        }
    }

    @Singleton
    public static class PortalAllocatorContext extends AllocatorContext {

        @Inject
        public PortalAllocatorContext(IamContext iam) {
            super(iam, "../portal/target/portal-1.0-SNAPSHOT.jar", "ai.lzy.portal.App", 23911);
        }

        @Override
        @PreDestroy
        public void close() {
            super.close();
        }
    }
}
