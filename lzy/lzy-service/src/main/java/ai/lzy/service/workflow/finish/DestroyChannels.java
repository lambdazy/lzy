package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class DestroyChannels implements Supplier<StepResult> {
    private final String execId;
    private final String idempotencyKey;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;
    private final Consumer<String> resultConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public DestroyChannels(String execId, @Nullable String idempotencyKey,
                           LzyChannelManagerPrivateBlockingStub channelsClient,
                           Consumer<String> resultConsumer,
                           Function<StatusRuntimeException, StepResult> failAction,
                           Logger log, String logPrefix)
    {
        this.execId = execId;
        this.idempotencyKey = idempotencyKey;
        this.channelsClient = channelsClient;
        this.resultConsumer = resultConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Destroy channels of execution: { execId: {} }", logPrefix, execId);

        var destroyAllChannelsClient = (idempotencyKey == null) ? channelsClient :
            withIdempotencyKey(channelsClient, idempotencyKey + "_destroy_exec_channels");
        try {
            var op = destroyAllChannelsClient.destroyAll(LCMPS.ChannelDestroyAllRequest.newBuilder()
                .setExecutionId(execId).build());
            resultConsumer.accept(op.getId());
            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in Channels::destroyAll call for execution with id='{}': {}", logPrefix, execId,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }
    }
}
