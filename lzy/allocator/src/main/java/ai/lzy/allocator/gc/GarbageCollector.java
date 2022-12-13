package ai.lzy.allocator.gc;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Named;

import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class GarbageCollector extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    public static final AtomicBoolean ENABLED = new AtomicBoolean(true);

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final VmAllocator allocator;
    private final SubjectServiceGrpcClient subjectClient;
    private final Timer timer = new Timer("gc-timer", true);

    public GarbageCollector(VmDao dao, VmAllocator allocator, ServiceConfig config,
                            @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("AllocatorOperationDao") OperationDao operationDao,
                            @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.vmDao = dao;
        this.allocator = allocator;
        this.operationsDao = operationDao;
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);

        LOG.info("Start GC with rate {}", config.getGcPeriod());
        timer.scheduleAtFixedRate(this, config.getGcPeriod().toMillis(), config.getGcPeriod().toMillis());
    }

    @Override
    public void run() {
        if (!ENABLED.get()) {
            return;
        }

        try {
            cleanExpiredVms();

            // TODO: drop `DEAD` rows
        } catch (Exception e) {
            LOG.error("Error during GC: " + e.getMessage(), e);
        }
    }

    private void cleanExpiredVms() throws SQLException {
        var expiredVms = vmDao.listExpired(10);
        LOG.info("Found {} expired entries", expiredVms.size());

        expiredVms.forEach(vm -> {
            try {
                LOG.info("Clean VM {}", vm);

                var allocOp = operationsDao.get(vm.allocOpId(), null);
                if (allocOp != null && !allocOp.done()) {
                    LOG.info("Clean VM {}: try to fail allocation operation {}...", vm.vmId(), allocOp.id());
                    var status = toProto(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired"));

                    var op = operationsDao.updateError(vm.allocOpId(), status.toByteArray(), null);
                    if (op == null) {
                        LOG.warn("Clean VM {}: allocate operation {} not found", vm.vmId(), vm.allocOpId());
                    }
                }

                var vmSubjectId = vm.allocateState().vmSubjectId();
                if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
                    LOG.info("Clean VM {}: removing IAM subject {}...", vm.vmId(), vmSubjectId);
                    subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                }

                allocator.deallocate(vm.vmId());
                //will retry deallocate if it fails
                vmDao.setStatus(vm.vmId(), Vm.Status.DEAD, null);
                LOG.info("Clean VM {}: done", vm.vmId());
            } catch (SQLException e) {
                if ("42P01".equals(e.getSQLState())) {
                    // `ERROR: relation <xxx> does not exist` -- not an issue in tests
                    LOG.debug("Error during clean up Vm {}", vm);
                } else {
                    LOG.error("Error during clean up Vm {}", vm, e);
                }
            } catch (Exception e) {
                LOG.error("Error during clean up Vm {}", vm, e);
            }
        });
    }

    public void shutdown() {
        timer.cancel();
    }
}
