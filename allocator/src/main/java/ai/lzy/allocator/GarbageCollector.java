package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.SQLException;
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
    public GarbageCollector(VmDao dao, OperationDao operations, VmAllocator allocator,
        ServiceConfig config, Storage storage) {
        this.dao = dao;
        this.operations = operations;
        this.allocator = allocator;
        this.storage = storage;
        timer.scheduleAtFixedRate(this, config.gcPeriod().toMillis(), config.gcPeriod().toMillis());
    }

    @Override
    public void run() {
        try {
            LOG.debug("Starting garbage collector");
            var vms = dao.getExpired(100, null);
            vms.forEach(vm -> {
                try (var tr = new TransactionHandle(storage)) {
                    var op = operations.get(vm.allocationOperationId(), tr);
                    if (op != null) {
                        operations.update(op.complete(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired")), tr);
                    }
                    tr.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                allocator.deallocate(vm);
                //will retry deallocate if it fails
                dao.update(new Vm.VmBuilder(vm).setState(Vm.State.DEAD).build(), null);
            });
        } catch (Exception e) {
            LOG.error("Error during GC", e);
        }
    }

    public void shutdown() {
        timer.cancel();
    }
}
