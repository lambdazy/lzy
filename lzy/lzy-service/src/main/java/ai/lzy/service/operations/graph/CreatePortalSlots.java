package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.portal.services.PortalService;
import ai.lzy.service.dao.DataFlowGraph.Data;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.lzy.portal.grpc.ProtoConverter.makePortalInputSlot;
import static ai.lzy.portal.grpc.ProtoConverter.makePortalOutputSlot;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public final class CreatePortalSlots extends ExecuteGraphContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final LMST.StorageConfig storageConfig;
    private final LzyPortalBlockingStub portalClient;

    public CreatePortalSlots(ExecutionStepContext stepCtx, ExecuteGraphState state,
                             LMST.StorageConfig storageConfig, LzyPortalBlockingStub portalClient)
    {
        super(stepCtx, state);
        this.storageConfig = storageConfig;
        this.portalClient = portalClient;
    }

    @Override
    public StepResult get() {
        log().info("{} Create portal slots to consume and supply data: { wfName: {}, execId: {} }", logPrefix(),
            wfName(), execId());

        final Map<String, Data> portalSlotName2data;

        if (portalSlotsNames() == null) {
            log().debug("{} Generate names for portal slots...", logPrefix());

            portalSlotName2data = generatePortalSlotsNames(dataFlowGraph().getDataflow());
            setPortalSlotsNames(portalSlotName2data.entrySet().stream().collect(Collectors.toMap(
                /* slotUri */ entry -> entry.getValue().slotUri(),
                /* slotName */ Map.Entry::getKey
            )));

            log().debug("{} Save generated names for portal slots...", logPrefix());

            try {
                saveState();
            } catch (Exception e) {
                return retryableFail(e, "Cannot save data about generated names for portal slots", Status.INTERNAL
                    .withDescription("Cannot create portal slots").asRuntimeException());
            }
        } else {
            log().debug("{} Names for portal slots are loaded from db...", logPrefix());

            portalSlotName2data = dataFlowGraph().getDataflow().stream().collect(Collectors.toMap(
                /* slotName */ data -> portalSlotsNames().get(data.slotUri()),
                /* data */ Function.identity()
            ));
        }

        log().debug("{} Build request to open portal slots...", logPrefix());

        var dataDescriptions = request().getDataDescriptionsList().stream().collect(Collectors.toMap(
            LWF.DataDescription::getStorageUri, Function.identity()));
        var request = LzyPortalApi.OpenSlotsRequest.newBuilder();

        for (Map.Entry<String, Data> entry : portalSlotName2data.entrySet()) {
            var slotName = entry.getKey();
            var data = entry.getValue();
            var dataDescription = dataDescriptions.get(data.slotUri());
            var dataScheme = dataDescription.hasDataScheme() ? dataDescription.getDataScheme() : null;
            var channelId = channels().get(data.slotUri());
            var portalSlotDesc = data.supplier() != null ?
                /* write data to portal */
                makePortalInputSlot(data.slotUri(), slotName, channelId, dataScheme, storageConfig) :
                /* read data from portal */
                makePortalOutputSlot(data.slotUri(), channelId, slotName, dataScheme, storageConfig);
            request.addSlots(portalSlotDesc);
        }

        log().debug("{} Send request to open portal slots for tasks...", logPrefix());

        try {
            var idempotentPortalClient = (idempotencyKey() == null) ? portalClient :
                withIdempotencyKey(portalClient, idempotencyKey() + "_open_tasks_data_slots");
            //noinspection ResultOfMethodCallIgnored
            idempotentPortalClient.openSlots(request.build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while PortalClient::openSlots call", sre);
        }

        return StepResult.CONTINUE;
    }

    private Map<String, Data> generatePortalSlotsNames(List<Data> outputs) {
        return outputs.stream().collect(Collectors.toMap(
            /* key */ data -> PortalService.PORTAL_SLOT_PREFIX + "_" + idGenerator().generate(),
            /* value */ Function.identity()
        ));
    }
}
