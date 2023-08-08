package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.workflow.LWFS.AbortWorkflowRequest;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.ManagedChannel;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.Objects;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

@Singleton
public class LzyServiceClient {
    private final LzyWorkflowServiceBlockingStub lzyServiceBlockingStub;
    private final ChannelDao channelDao;

    public LzyServiceClient(@Named("ChannelManagerWorkflowGrpcChannel") ManagedChannel lzyServiceChannel,
                            @Named("ChannelManagerIamToken") RenewableJwt token, ChannelDao channelDao)
    {
        this.channelDao = channelDao;
        this.lzyServiceBlockingStub = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceChannel),
            ChannelManagerMain.APP, () -> token.get().token());
    }

    /**
     * Aborts the workflow associated with the channel and drops the channel.
     * Must be not very long call
     */
    public void destroyChannelAndWorkflow(String channelId, String reason, @Nullable String idempotencyKey,
                                          @Nullable TransactionHandle tx)
        throws SQLException
    {
        var channel = channelDao.drop(channelId, tx);
        if (channel == null) {
            return;
        }

        var lzyClient = Objects.nonNull(idempotencyKey) ?
            withIdempotencyKey(lzyServiceBlockingStub, idempotencyKey) :
            lzyServiceBlockingStub;

        lzyClient.abortWorkflow(AbortWorkflowRequest.newBuilder()
            .setExecutionId(channel.executionId())
            .setWorkflowName(channel.workflowName())
            .setReason(reason)
            .build());
    }
}
