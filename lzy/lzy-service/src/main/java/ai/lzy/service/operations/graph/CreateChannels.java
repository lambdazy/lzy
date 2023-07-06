package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.db.DbHelper;
import ai.lzy.service.dao.DataFlowGraph.Data;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMST;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class CreateChannels extends ExecuteGraphContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final LzyChannelManagerPrivateBlockingStub channelsClient;

    public CreateChannels(ExecutionStepContext stepCtx, ExecuteGraphState state,
                          LzyChannelManagerPrivateBlockingStub channelsClient)
    {
        super(stepCtx, state);
        this.channelsClient = channelsClient;
    }

    @Override
    public StepResult get() {
        if (channels() != null) {
            log().debug("{} Channels already created, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Create channels for slots with data...", logPrefix());

        Map<Boolean, List<Data>> dataPartitionBySupplier = dataFlowGraph().getDataflow().stream()
            .collect(Collectors.partitioningBy(data -> data.producer() != null));

        List<Data> tasksOutput = dataPartitionBySupplier.get(true);
        List<Data> dataFromStorage = dataPartitionBySupplier.get(false);

        log().debug("{} Request to create channels for data...", logPrefix());

        final Map<String, String> slotUri2channelId = new HashMap<>();
        final LMST.StorageConfig storageConfig;

        try {
            storageConfig = DbHelper.withRetries(log(), () -> execDao().getStorageConfig(execId(), null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot get storage config", Status.INTERNAL
                .withDescription("Cannot create channels").asRuntimeException());
        }

        var storagePeerBuilder = LC.PeerDescription.StoragePeer.newBuilder();

        if (storageConfig.hasS3()) {
            storagePeerBuilder.setS3(storageConfig.getS3());
        } else {
            storagePeerBuilder.setAzure(storageConfig.getAzure());
        }

        var createRequestBuilder = LCMPS.GetOrCreateRequest.newBuilder()
            .setUserId(userId())
            .setWorkflowName(wfName())
            .setExecutionId(execId());

        try {
            for (var data : tasksOutput) {
                var idempotentChannelsClient = (idempotencyKey() == null) ? channelsClient :
                    withIdempotencyKey(channelsClient, idempotencyKey() + "_" + data.storageUri());

                var res = idempotentChannelsClient.getOrCreate(
                    createRequestBuilder
                    .setConsumer(storagePeerBuilder
                        .setStorageUri(data.storageUri())
                        .build())
                    .build());

                slotUri2channelId.put(data.storageUri(), res.getChannelId());
            }

            for (var data: dataFromStorage) {
                var idempotentChannelsClient = (idempotencyKey() == null) ? channelsClient :
                    withIdempotencyKey(channelsClient, idempotencyKey() + "_" + data.storageUri());

                var res = idempotentChannelsClient.getOrCreate(
                    createRequestBuilder
                    .setProducer(storagePeerBuilder
                        .setStorageUri(data.storageUri())
                        .build())
                    .build());

                slotUri2channelId.put(data.storageUri(), res.getChannelId());
            }


        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Cannot create channels for slots", sre);
        }

        log().debug("{} Channel successfully created. Save data to dao...", logPrefix());
        setChannels(slotUri2channelId);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save data about created channels", Status.INTERNAL.withDescription(
                "Cannot create channels").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
