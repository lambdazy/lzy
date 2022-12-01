package ai.lzy.service.gc;

import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.TimerTask;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);
    private final String id;
    private final WorkflowDao workflowDao;

    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;


    public GarbageCollectorTask(String id, WorkflowDao workflowDao,
                                AllocatorGrpc.AllocatorBlockingStub allocatorClient,
                                LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.id = id;
        this.workflowDao = workflowDao;
        this.allocatorClient = allocatorClient;
        this.channelManagerClient = channelManagerClient;
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
                        LOG.info("Cleaning portal {} for execution {}", portalDesc, execution);
                        channelManagerClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                            .setChannelId(portalDesc.stderrChannelId())
                            .build());
                        channelManagerClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                            .setChannelId(portalDesc.stdoutChannelId())
                            .build());
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
                    withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.setDeadExecutionStatus(execution,
                        Timestamp.from(Instant.now())));
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
