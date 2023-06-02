package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.workflow.LWFS.AbortWorkflowRequest;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.ManagedChannel;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.sql.SQLException;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

@Singleton
public class LzyServiceClient {
    private final LzyWorkflowServiceBlockingStub lzyServiceBlockingStub;
    private final ChannelDao channelDao;

    public LzyServiceClient(@Named("ChannelManagerWorkflowGrpcChannel") ManagedChannel lzyServiceChannel,
                            @Named("ChannelManagerIamToken") RenewableJwt token, ChannelDao channelDao)
    {
        this.channelDao = channelDao;
        this.lzyServiceBlockingStub = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceChannel),
            "LzyServiceStub", () -> token.get().token());
    }

    public void destroyChannelAndWorkflow(String channelId, String reason, TransactionHandle tx) throws SQLException {
        var channel = channelDao.drop(channelId, tx);
        if (channel == null) {
            return;
        }

        lzyServiceBlockingStub.abortWorkflow(AbortWorkflowRequest.newBuilder()
            .setExecutionId(channel.executionId())
            .setWorkflowName(channel.workflowName())
            .setReason(reason)
            .build());
    }
}
