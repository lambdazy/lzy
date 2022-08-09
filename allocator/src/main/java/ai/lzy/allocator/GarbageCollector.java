package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.services.AllocatorApi;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class GarbageCollector extends Thread {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final AtomicBoolean stopping = new AtomicBoolean(true);
    private final ServiceConfig config;


    @Inject
    public GarbageCollector(VmDao dao, OperationDao operations, VmAllocator allocator, ServiceConfig config) {
        super("allocator-garbage-collector");
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
        this.config = config;
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            LOG.debug("Starting garbage collector");
            try {
                Thread.sleep(config.gcPeriod().toMillis());
            } catch (InterruptedException e) {
                continue;
            }
            var vms = dao.getExpired(100);
            vms.forEach(vm -> {
                allocator.deallocate(vm);
                dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build());
                var op = operations.get(vm.allocationOperationId());
                if (op != null) {
                    operations.update(op.complete(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired")));
                }
            });
        }
    }
}
