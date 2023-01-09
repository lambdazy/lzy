package ai.lzy.service.gc;

import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);
    private final String id;
    private final WorkflowDao workflowDao;

    private final RenewableJwt internalCreds;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;

    public GarbageCollectorTask(String id, WorkflowDao workflowDao,
                                RenewableJwt internalUserCredentials,
                                ManagedChannel allocatorChannel)
    {
        this.id = id;
        this.workflowDao = workflowDao;
        this.internalCreds = internalUserCredentials;
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
    }

    @Override
    public void run() {
        try {
            String expiredExecution = workflowDao.getExpiredExecution();
            while (expiredExecution != null) {
                cleanWorkflow(expiredExecution);
                expiredExecution = workflowDao.getExpiredExecution();
            }
        } catch (Exception e) {
            LOG.error("Got error during GC {} task", id, e);
        }
    }

    private void cleanWorkflow(String execution) {
        LOG.info("Execution {} is expired, GC {}", execution, id);

        PortalDescription portalDesc = null;
        try {
            portalDesc = withRetries(LOG, () -> workflowDao.getPortalDescription(execution));
        } catch (Exception e) {
            LOG.error("Cannot get portal for execution {}", execution, e);
            return;
        }

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            try {
                var portalAddress = portalDesc.vmAddress();
                LOG.info("Finishing portal {} for execution {}", portalAddress, execution);

                var portalChannel = newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);
                var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
                    APP, () -> internalCreds.get().token());
                var ignored = portalClient.stop(Empty.getDefaultInstance());

                portalChannel.shutdown();
                portalChannel.awaitTermination(10, TimeUnit.SECONDS);

                workflowDao.updateAllocatedVmAddress(execution, null, null);

                LOG.info("Portal {} finished for execution {}", portalAddress, execution);
            } catch (Exception e) {
                LOG.error("Cannot clean portal for execution {}", execution, e);
            }
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            try {
                LOG.info("Freeing vm {} for execution {}", portalDesc.vmId(), execution);

                allocatorClient.free(VmAllocatorApi.FreeRequest.newBuilder()
                    .setVmId(portalDesc.vmId())
                    .build());

                workflowDao.updateAllocateOperationData(execution, null, null);

                LOG.info("VM cleaned {} for execution {}", portalDesc.vmId(), execution);
            } catch (Exception e) {
                LOG.error("Cannot free VM for execution {}", execution, e);
            }
        }

        try {
            var session = withRetries(LOG, () -> workflowDao.getAllocatorSession(execution));
            if (session != null) {
                LOG.info("Cleaning allocator session {} for execution {}", session, execution);
                allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                    .setSessionId(session)
                    .build());

                workflowDao.updateAllocatorSession(execution, null, null);
                LOG.info("Allocator session {} cleaned for execution {}", session, execution);
            }
        } catch (Exception e) {
            LOG.error("Cannot clean allocator session for execution {}", execution, e);
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.setDeadExecutionStatus(execution));
            LOG.info("Execution {} is cleaned, GC {}", execution, id);
        } catch (Exception e) {
            LOG.error("Cannot update execution status {}", execution, e);
        }
    }
}
