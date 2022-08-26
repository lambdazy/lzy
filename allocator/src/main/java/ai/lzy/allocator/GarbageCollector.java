package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;

@Singleton
public class GarbageCollector extends TimerTask {

    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final Timer timer = new Timer("gc-timer", true);
    private final Storage storage;


    @Inject
    public GarbageCollector(VmDao dao, OperationDao operations, VmAllocator allocator, ServiceConfig config,
                            AllocatorDataSource storage)
    {
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
        this.storage = storage;
        timer.scheduleAtFixedRate(this, config.getGcPeriod().toMillis(), config.getGcPeriod().toMillis());
    }

    @Override
    public void run() {
        try {
            var vms = dao.getExpired(100, null);
            LOG.debug("Found {} expired entries", vms.size());
            vms.forEach(vm -> {
                try {
                    LOG.info("Vm {} is expired", vm);
                    try (var tr = TransactionHandle.create(storage)) {
                        var op = operations.get(vm.allocationOperationId(), tr);
                        if (op != null) {
                            operations.update(op.complete(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired")),
                                tr);
                        } else {
                            LOG.warn("Op with id={} not found", vm.allocationOperationId());
                        }
                        tr.commit();
                    }
                    allocator.deallocate(vm);
                    //will retry deallocate if it fails
                    dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build(), null);
                } catch (Exception e) {
                    LOG.error("Error during clean up Vm {}", vm, e);
                }
            });
        } catch (Exception e) {
            LOG.error("Error during GC: " + e.getMessage(), e);
        }
    }

    public void shutdown() {
        timer.cancel();
    }
}
