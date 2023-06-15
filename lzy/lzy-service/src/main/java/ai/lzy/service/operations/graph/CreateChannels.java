package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.DataFlowGraph.Data;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
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
            .collect(Collectors.partitioningBy(data -> data.supplier() != null));

        List<Data> tasksOutput = dataPartitionBySupplier.get(true);
        List<Data> dataFromPortal = dataPartitionBySupplier.get(false);

        log().debug("{} Generate channels names for data from other graph tasks...", logPrefix());
        Map<String, Data> readFromWorker = generateChannelsNamesForTaskOutputs(tasksOutput);

        final Map<String, Data> readFromPortal;
        if (volatileChannelsNames() == null) {
            log().debug("{} Generate channels names for data from portal...", logPrefix());

            readFromPortal = generateChannelsNamesForPortalData(dataFromPortal);
            setVolatileChannelsNames(readFromPortal.entrySet().stream().collect(Collectors.toMap(
                /* slotUri */ entry -> entry.getValue().slotUri(),
                /* channelName */ Map.Entry::getKey
            )));

            log().debug("{} Save generated channels names for data from portal...", logPrefix());

            try {
                saveState();
            } catch (Exception e) {
                return retryableFail(e, "Cannot save data about generated channel names for data from portal slots",
                    Status.INTERNAL.withDescription("Cannot create channels for data from portal")
                        .asRuntimeException());
            }
        } else {
            log().debug("{} Channels names for data from portal are loaded from db...", logPrefix());

            readFromPortal = dataFromPortal.stream().collect(Collectors.toMap(
                /* channelName */ data -> volatileChannelsNames().get(data.slotUri()),
                /* Data */ Function.identity()
            ));
        }

        log().debug("{} Request to create channels for data...", logPrefix());

        final Map<String, String> slotUri2channelId;
        try {
            slotUri2channelId = Stream.concat(readFromWorker.entrySet().stream(), readFromPortal.entrySet().stream())
                .collect(Collectors.toMap(
                    /* slotUri */ entry -> entry.getValue().slotUri(),
                    /* channelId */ entry -> createChannel(entry.getKey())
                ));
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Cannot create channels for data from portal or graph task output slots", sre);
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

    private Map<String, Data> generateChannelsNamesForTaskOutputs(Collection<Data> outputs) {
        return outputs.stream().collect(Collectors.toMap(data -> "channel_" + data.slotUri(), Function.identity()));
    }

    private Map<String, Data> generateChannelsNamesForPortalData(Collection<Data> outputs) {
        /*
        todo: At this moment it's forbidden to read data from slot multiple times, so I suggest a solution to create
              a new portal slot and channel with UUID containing name for each graph.
         */
        return outputs.stream().collect(Collectors.toMap(
            /* key */ data -> idGenerator().generate("portal_channel_" + data.slotUri() + "_"),
            /* value */ Function.identity())
        );
    }

    private String createChannel(String channelName) throws StatusRuntimeException {
        var idempotentChannelsClient = (idempotencyKey() == null) ? channelsClient :
            withIdempotencyKey(channelsClient, idempotencyKey() + "_" + channelName);

        var request = makeCreateChannelCommand(userId(), wfName(), execId(), channelName);
        var response = idempotentChannelsClient.create(request);

        return response.getChannelId();
    }
}
