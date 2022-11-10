package ai.lzy.allocator;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.Vm;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.SimpleOperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;
import javax.inject.Named;

@Singleton
public class GarbageCollector extends TimerTask {

    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final VmDao dao;
    private final OperationDao operations;
    private final VmAllocator allocator;
    private final SubjectServiceGrpcClient subjectClient;
    private final Timer timer = new Timer("gc-timer", true);
    private final Storage storage;

    public GarbageCollector(VmDao dao, VmAllocator allocator, ServiceConfig config,
                            AllocatorDataSource storage, @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.dao = dao;
        this.allocator = allocator;
        this.storage = storage;
        this.operations = new SimpleOperationDao(storage);
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);

        timer.scheduleAtFixedRate(this, config.getGcPeriod().toMillis(), config.getGcPeriod().toMillis());
    }

    @Override
    public void run() {
        try {
            var vms = dao.listExpired(100);
            LOG.debug("Found {} expired entries", vms.size());
            vms.forEach(vm -> {
                try {
                    LOG.info("Vm {} is expired", vm);
                    try (var tr = TransactionHandle.create(storage)) {
                        var status = com.google.rpc.Status.newBuilder()
                            .setCode(Status.DEADLINE_EXCEEDED.getCode().value())
                            .setMessage("Vm is expired")
                            .build();

                        var op = operations.updateError(vm.allocationOperationId(), status.toByteArray(), tr);
                        if (op == null) {
                            LOG.warn("Op with id={} not found", vm.allocationOperationId());
                        }
                        tr.commit();
                    }

                    var vmSubjectId = vm.state().vmSubjectId();
                    if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
                        subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                    }

                    allocator.deallocate(vm.vmId());
                    //will retry deallocate if it fails
                    dao.updateStatus(vm.vmId(), Vm.VmStatus.DEAD, null);
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
