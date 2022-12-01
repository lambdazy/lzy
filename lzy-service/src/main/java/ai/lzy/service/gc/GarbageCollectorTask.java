package ai.lzy.service.gc;

import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
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
                LOG.info("Execution {} is expired", expiredExecution);
                final var execution = expiredExecution;

                try {
                    var portalDesc = withRetries(LOG, () -> workflowDao.getPortalDescription(execution));
                    if (portalDesc != null) {
                        var portalAddress = portalDesc.vmAddress();
                        var portalChannel = newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);

                        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
                            APP, () -> internalCreds.get().token());
                        var ignored = portalClient.finish(LzyPortalApi.FinishRequest.newBuilder().build());

                        portalChannel.shutdown();
                        portalChannel.awaitTermination(10, TimeUnit.SECONDS);

                        allocatorClient.free(VmAllocatorApi.FreeRequest.newBuilder()
                            .setVmId(portalDesc.vmId())
                            .build());
                    }
                } catch (Exception e) {
                    LOG.error("Cannot clean portal for execution {}", execution, e);
                }

                try {
                    var session = withRetries(LOG, () -> workflowDao.getAllocatorSession(execution));
                    if (session != null) {
                        LOG.info("Cleaning allocator session {} for execution {}", session, execution);
                        allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                            .setSessionId(session)
                            .build());
                    }
                } catch (Exception e) {
                    LOG.error("Cannot clean allocator session for execution {}", execution, e);
                }

                try {
                    withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.setDeadExecutionStatus(execution));
                    LOG.info("Execution {} is cleaned", execution);
                } catch (Exception e) {
                    LOG.error("Cannot update execution status {}", execution, e);
                }

                expiredExecution = workflowDao.getExpiredExecution();
            }
        } catch (Exception e) {
            LOG.error("Got error during GC {} task", id, e);
        }
    }
}
