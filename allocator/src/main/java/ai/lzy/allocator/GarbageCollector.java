package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.model.Vm;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.ManagedChannel;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Named;

@Singleton
public class GarbageCollector extends TimerTask {

    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    public static final AtomicBoolean ENABLED = new AtomicBoolean(true);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final SubjectServiceGrpcClient subjectClient;
    private final Timer timer = new Timer("gc-timer", true);
    private final Storage storage;

    public GarbageCollector(VmDao dao, VmAllocator allocator, ServiceConfig config, AllocatorDataSource storage,
                            @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("AllocatorOperationDao") OperationDao operationDao,
                            @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.dao = dao;
        this.allocator = allocator;
        this.storage = storage;
        this.operations = operationDao;
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);

        timer.scheduleAtFixedRate(this, config.getGcPeriod().toMillis(), config.getGcPeriod().toMillis());
    }

    @Override
    public void run() {
        if (!ENABLED.get()) {
            return;
        }

        try {
            var vms = dao.listExpired(100);
            LOG.debug("Found {} expired entries", vms.size());
            vms.forEach(vm -> {
                try {
                    LOG.info("Vm {} is expired", vm);
                    try (var tr = TransactionHandle.create(storage)) {
                        var status = com.google.rpc.Status.newBuilder()
                            .setCode(io.grpc.Status.DEADLINE_EXCEEDED.getCode().value())
                            .setMessage("Vm is expired")
                            .build();

                        var op = operations.updateError(vm.allocOpId(), status.toByteArray(), tr);
                        if (op == null) {
                            LOG.warn("Op with id={} not found", vm.allocOpId());
                        }
                        tr.commit();
                    }

                    var vmSubjectId = vm.allocateState().vmSubjectId();
                    if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
                        subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                    }

                    allocator.deallocate(vm.vmId());
                    //will retry deallocate if it fails
                    dao.setStatus(vm.vmId(), Vm.Status.DEAD, null);
                } catch (SQLException e) {
                    if ("42P01".equals(e.getSQLState())) {
                        // ERROR: relation <xxx> does not exist
                        // not an issue in tests
                        LOG.debug("Error during clean up Vm {}", vm);
                    } else {
                        LOG.error("Error during clean up Vm {}", vm, e);
                    }
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
