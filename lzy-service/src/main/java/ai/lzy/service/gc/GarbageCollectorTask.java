package ai.lzy.service.gc;

import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.dao.GcDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.TimerTask;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);
    private final String id;
    private final GcDao gcDao;
    private final WorkflowDao workflowDao;
    private final Storage storage;

    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;


    public GarbageCollectorTask(String id, GcDao gcDao, WorkflowDao workflowDao, Storage storage,
                                AllocatorGrpc.AllocatorBlockingStub allocatorClient,
                                LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.id = id;
        this.gcDao = gcDao;
        this.workflowDao = workflowDao;
        this.storage = storage;
        this.allocatorClient = allocatorClient;
        this.channelManagerClient = channelManagerClient;
    }

    @Override
    public void run() {
        try {
            try (var tr = TransactionHandle.create(storage)) {
                gcDao.updateStatus(tr, id);
                tr.commit();
            }

            List<String> expiredExecutions = workflowDao.listExpiredExecutions(100);
            LOG.info("GC {} found {} expired executions", id, expiredExecutions.size());

            expiredExecutions.forEach(execution -> {
                try {
                    LOG.info("Execution {} is expired", execution);

                    try {
                        var portalDesc = workflowDao.getPortalDescription(execution);
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
                    } catch (SQLException e) {
                        LOG.info("Cannot clean portal for execution {}", execution, e);
                    }

                    try {
                        var session = workflowDao.getAllocatorSession(execution);
                        if (session != null) {
                            LOG.info("Cleaning allocator session {} for execution {}", session, execution);
                            allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                                .setSessionId(session)
                                .build());
                        }
                    } catch (SQLException e) {
                        LOG.info("Cannot clean allocator session for execution {}", execution, e);
                    }

                    String userId = null;
                    String workflowName = null;
                    try {
                        userId = workflowDao.getUserId(execution);
                        workflowName = workflowDao.getWorkflowName(execution);
                    } catch (SQLException e) {
                        LOG.info("Cannot find userId or workflowName to update active execution {}", execution, e);
                    }

                    String finalUserId = userId;
                    String finalWorkflowName = workflowName;
                    withRetries(defaultRetryPolicy(), LOG, () -> {
                        try (var transaction = TransactionHandle.create(storage)) {
                            workflowDao.setDeadExecutionStatus(execution, Timestamp.from(Instant.now()), transaction);

                            if (finalUserId != null && finalWorkflowName != null) {
                                workflowDao.updateActiveExecution(finalUserId, finalWorkflowName, execution, null);
                            }
                            transaction.commit();
                        }
                    });
                    LOG.info("Execution {} is cleaned", execution);
                } catch (Exception e) {
                    LOG.error("Cannot update execution status {}", execution, e);
                }
            });
        } catch (SQLException e) {
            LOG.error("Got error during GC {} task", id, e);
        }
    }
}
