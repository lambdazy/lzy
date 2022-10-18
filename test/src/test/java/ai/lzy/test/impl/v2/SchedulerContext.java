package ai.lzy.test.impl.v2;

import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.test.impl.Utils;
import com.google.common.net.HostAndPort;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class SchedulerContext {
    private static final int SCHEDULER_PORT = 12759;

    private final AllocatorContext allocator;
    private final IamContext iam;
    private final ChannelManagerContext channelManager;
    private final HostAndPort address;
    private final ApplicationContext context;
    private final SchedulerApi scheduler;

    @Inject
    public SchedulerContext(AllocatorContext allocator, IamContext iam, ChannelManagerContext channelManager) {
        this.allocator = allocator;
        this.iam = iam;

        this.channelManager = channelManager;

        this.address = HostAndPort.fromParts("localhost", SCHEDULER_PORT);
        final var opts = Utils.createModuleDatabase("scheduler");
        opts.putAll(new HashMap<String, Object>(Map.of(
                "scheduler.scheduler-address", address,
                "scheduler.port", SCHEDULER_PORT,
                "scheduler.allocator-address", allocator.address(),
                "scheduler.channel-manager-address", channelManager.address(),
                "scheduler.iam.address", iam.address()
        )));
        this.context = ApplicationContext.run(opts);
        this.scheduler = this.context.getBean(SchedulerApi.class);
    }

    @PreDestroy
    public void close() {
        scheduler.close();
        try {
            scheduler.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        context.stop();
    }

    public HostAndPort address() {
        return address;
    }
}
