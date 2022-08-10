package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionManager;
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
    private final TransactionManager transactions;


    @Inject
    public GarbageCollector(VmDao dao, OperationDao operations, VmAllocator allocator, ServiceConfig config,
            TransactionManager transactions) {
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
        this.transactions = transactions;
        timer.scheduleAtFixedRate(this, config.gcPeriod().toMillis(), config.gcPeriod().toMillis());
    }

    @Override
    public void run() {
        LOG.debug("Starting garbage collector");
        var vms = dao.getExpired(100, null);
        vms.forEach(vm -> {
            try (var tr = transactions.start()) {
                dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build(), tr);
                allocator.deallocate(vm);
                var op = operations.get(vm.allocationOperationId(), tr);
                if (op != null) {
                    operations.update(op.complete(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired")), tr);
                }
            } catch (Exception e) {
                LOG.error("Cannot deallocate vm {}", vm, e);
            }
        });
    }

    public void shutdown() {
        timer.cancel();
    }
}
