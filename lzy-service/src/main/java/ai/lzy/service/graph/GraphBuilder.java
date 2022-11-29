package ai.lzy.service.graph;

import ai.lzy.model.slot.Slot;
import ai.lzy.portal.Portal;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutor.SlotToChannelAssignment;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.model.grpc.ProtoConverter.*;
import static ai.lzy.portal.grpc.ProtoConverter.*;

class GraphBuilder {
    private static final Logger LOG = LogManager.getLogger(GraphBuilder.class);

    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    public GraphBuilder(WorkflowDao workflowDao, ExecutionDao executionDao,
                        LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.channelManagerClient = channelManagerClient;
    }

    public void build(GraphExecutionState state, LzyPortalGrpc.LzyPortalBlockingStub portalClient) {
        var executionId = state.getExecutionId();
        var workflowName = state.getWorkflowName();
        var dataFlow = state.getDataFlowGraph().getDataFlow();
        var slot2description = state.getDescriptions().stream()
            .collect(Collectors.toMap(LWF.DataDescription::getStorageUri, Function.identity()));


        Map<String, String> slot2channelId;
        try {
            slot2channelId = createChannelsForDataFlow(executionId, dataFlow, slot2description, portalClient, state);
        } catch (StatusRuntimeException e) {
            state.fail(e.getStatus(), "Cannot build graph");
            LOG.error("Cannot assign slots to channels for execution: " +
                "{ executionId: {}, workflowName: {} }, error: {} ", executionId, workflowName, e.getMessage());
            return;
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot build graph");
            LOG.error("Cannot assign slots to channels for execution: " +
                "{ executionId: {}, workflowName: {} }, error: {} ", executionId, workflowName, e.getMessage());
            return;
        }

        List<TaskDesc> tasks;
        try {
            tasks = buildTasksWithZone(executionId, state.getZone(), state.getOperations(),
                slot2channelId, slot2description, portalClient);
        } catch (StatusRuntimeException e) {
            state.fail(e.getStatus(), "Cannot build graph");
            LOG.error("Cannot build tasks for execution: { executionId: {}, workflowName: {} }, error: {} ",
                executionId, workflowName, e.getMessage());
            return;
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot build graph");
            LOG.error("Cannot build tasks for execution: { executionId: {}, workflowName: {} }, error: {} ",
                executionId, workflowName, e.getMessage());
            return;
        }

        state.setTasks(tasks);

        var channelIds = new HashSet<>(slot2channelId.values());

        var channelsDescriptions = channelIds.stream()
            .map(id -> GraphExecutor.ChannelDesc
                .newBuilder()
                .setId(id)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build())
            .toList();

        state.setChannels(channelsDescriptions);
    }

    private Map<String, String> createChannelsForDataFlow(String executionId, List<DataFlowGraph.Data> dataFlow,
                                                          Map<String, LWF.DataDescription> slot2dataDescription,
                                                          LzyPortalGrpc.LzyPortalBlockingStub portalClient,
                                                          GraphExecutionState state)
    {
        var slotName2channelId = new HashMap<String, String>();
        var partitionBySupplier = dataFlow.stream().collect(Collectors.partitioningBy(data -> data.supplier() != null));
        var fromOutput = partitionBySupplier.get(true);
        var fromPortal = partitionBySupplier.get(false);

        LMS3.S3Locator storageLocator;
        try {
            storageLocator = withRetries(LOG, () -> workflowDao.getStorageLocator(executionId));
        } catch (Exception e) {
            LOG.error("Cannot obtain information about snapshots storage for execution: { executionId: {} } " +
                e.getMessage(), executionId);
            throw new RuntimeException(e);
        }

        var portalSlotToOpen = new ArrayList<LzyPortal.PortalSlotDesc>();

        var inputSlotNames = new ArrayList<String>();

        for (var data : fromOutput) {
            var slotUri = data.slotUri();
            var channelId = channelManagerClient
                .create(makeCreateDirectChannelCommand(executionId, "channel_" + slotUri))
                .getChannelId();
            var portalInputSlotName = Portal.PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
            var dataDescription = slot2dataDescription.get(slotUri);

            inputSlotNames.add(portalInputSlotName);
            portalSlotToOpen.add(makePortalInputSlot(slotUri, portalInputSlotName, channelId, storageLocator));

            slotName2channelId.put(data.supplier(), channelId);
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }
        }
        state.setPortalInputSlots(inputSlotNames);

        Map<String, String> outputSlot2channel;

        synchronized (this) {
            try {
                var slotsUriAsOutput = fromPortal.stream().map(DataFlowGraph.Data::slotUri).collect(Collectors.toSet());
                var unknownFromPortal = withRetries(LOG, () ->
                    executionDao.retainNonExistingSlots(executionId, slotsUriAsOutput));

                withRetries(defaultRetryPolicy(), LOG, () -> executionDao.saveSlots(executionId, unknownFromPortal));
            } catch (Exception e) {
                LOG.error("Cannot save information to internal storage: " + e.getMessage());
                throw new RuntimeException(e);
            }

            try {
                var slotsUri = dataFlow.stream().map(DataFlowGraph.Data::slotUri).collect(Collectors.toSet());
                outputSlot2channel = withRetries(LOG, () -> executionDao.findChannels(slotsUri));
            } catch (Exception e) {
                LOG.error("Cannot obtain information about channels for slots. Execution: { executionId: {} } " +
                    e.getMessage(), executionId);
                throw new RuntimeException(e);
            }

            var newChannels = new HashMap<String, String>();
            var withoutOpenedPortalSlot = fromPortal.stream()
                .filter(data -> !outputSlot2channel.containsKey(data.slotUri())).toList();

            for (var data : withoutOpenedPortalSlot) {
                var slotUri = data.slotUri();
                var portalOutputSlotName = Portal.PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
                var channelId = channelManagerClient
                    .create(
                        makeCreateDirectChannelCommand(executionId, "portal_channel_" + slotUri))
                    .getChannelId();

                newChannels.put(slotUri, channelId);
                portalSlotToOpen.add(makePortalOutputSlot(slotUri, portalOutputSlotName, channelId, storageLocator));
            }

            try {
                withRetries(defaultRetryPolicy(), LOG, () -> executionDao.saveChannels(newChannels));
            } catch (Exception e) {
                LOG.error("Cannot save information about channels for execution: { executionId: {} } " +
                    e.getMessage(), executionId);
                throw new RuntimeException(e);
            }

            outputSlot2channel.putAll(newChannels);
        }

        for (var data : fromPortal) {
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, outputSlot2channel.get(data.slotUri()));
                }
            }
        }

        //noinspection ResultOfMethodCallIgnored
        portalClient.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder().addAllSlots(portalSlotToOpen).build());

        try {
            var slotsUriAsOutput = fromOutput.stream().map(DataFlowGraph.Data::slotUri).collect(Collectors.toSet());
            withRetries(defaultRetryPolicy(), LOG, () -> executionDao.saveSlots(executionId, slotsUriAsOutput));
        } catch (Exception e) {
            LOG.error("Cannot save information to internal storage: " + e.getMessage());
            throw new RuntimeException(e);
        }

        return slotName2channelId;
    }

    private List<TaskDesc> buildTasksWithZone(String executionId, String zoneName,
                                              Collection<LWF.Operation> operations,
                                              Map<String, String> slot2Channel,
                                              Map<String, LWF.DataDescription> slot2description,
                                              LzyPortalGrpc.LzyPortalBlockingStub portalClient)
    {
        var tasks = new ArrayList<TaskDesc>(operations.size());

        for (var operation : operations) {
            // TODO: ssokolvyak -- must not be generated here but passed in executeGraph request
            var taskId = UUID.randomUUID().toString();

            var channelNameForStdoutSlot = "channel_" + taskId + ":" + Slot.STDOUT_SUFFIX;
            var stdoutChannelId = channelManagerClient.create(
                makeCreateDirectChannelCommand(executionId, channelNameForStdoutSlot)).getChannelId();

            var channelNameForStderrSlot = "channel_" + taskId + ":" + Slot.STDERR_SUFFIX;
            var stderrChannelId = channelManagerClient.create(
                makeCreateDirectChannelCommand(executionId, channelNameForStderrSlot)).getChannelId();

            tasks.add(buildTaskWithZone(taskId, operation, zoneName, stdoutChannelId, stderrChannelId, slot2Channel,
                slot2description, portalClient));
        }

        return tasks;
    }

    private TaskDesc buildTaskWithZone(String taskId, LWF.Operation operation,
                                       String zoneName, String stdoutChannelId, String stderrChannelId,
                                       Map<String, String> slot2Channel,
                                       Map<String, LWF.DataDescription> slot2description,
                                       LzyPortalGrpc.LzyPortalBlockingStub portalClient)
    {
        var inputSlots = new ArrayList<LMS.Slot>();
        var outputSlots = new ArrayList<LMS.Slot>();
        var slotToChannelAssignments = new ArrayList<SlotToChannelAssignment>();

        BiConsumer<Collection<SlotDescription>, Boolean> slotsDescriptionsConsumer = (slotDescriptions, isInput) -> {
            for (var slotDescription : slotDescriptions) {
                var uri = slotDescription.getStorageUri();
                var slotName = slotDescription.getPath();
                var description = slot2description.get(uri);

                var hasDataScheme = description != null && description.hasDataScheme();

                if (isInput) {
                    var slot = hasDataScheme ? buildFileInputSlot(slotName, description.getDataScheme()) :
                        buildFileInputPlainContentSlot(slotName);
                    inputSlots.add(slot);
                } else {
                    var slot = hasDataScheme ? buildFileOutputSlot(slotName, description.getDataScheme()) :
                        buildFileOutputPlainContentSlot(slotName);
                    outputSlots.add(slot);
                }

                slotToChannelAssignments.add(SlotToChannelAssignment.newBuilder()
                    .setSlotName(slotName)
                    .setChannelId(slot2Channel.get(slotName))
                    .build());
            }
        };

        slotsDescriptionsConsumer.accept(operation.getInputSlotsList(), true);
        slotsDescriptionsConsumer.accept(operation.getOutputSlotsList(), false);

        var stdoutPortalSlotName = Portal.PORTAL_SLOT_PREFIX + "_" + taskId + ":" + Slot.STDOUT_SUFFIX;
        var stderrPortalSlotName = Portal.PORTAL_SLOT_PREFIX + "_" + taskId + ":" + Slot.STDERR_SUFFIX;

        //noinspection ResultOfMethodCallIgnored
        portalClient.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(makePortalInputStdoutSlot(taskId, stdoutPortalSlotName, stdoutChannelId))
            .addSlots(makePortalInputStderrSlot(taskId, stderrPortalSlotName, stderrChannelId))
            .build());

        var requirements = LMO.Requirements.newBuilder()
            .setZone(zoneName).setPoolLabel(operation.getPoolSpecName()).build();
        var env = LME.EnvSpec.newBuilder();

        if (!operation.getDockerImage().isBlank()) {
            env.setBaseEnv(LME.BaseEnv.newBuilder().setName(operation.getDockerImage()).build());
        }
        if (operation.hasPython()) {
            env.setAuxEnv(LME.AuxEnv.newBuilder().setPyenv(
                    LME.PythonEnv.newBuilder()
                        .setYaml(operation.getPython().getYaml())
                        .addAllLocalModules(
                            operation.getPython().getLocalModulesList().stream()
                                .map(module -> LME.LocalModule.newBuilder()
                                    .setName(module.getName())
                                    .setUri(module.getUrl())
                                    .build()
                                ).toList()
                        ).build())
                .build());
        }

        var taskOperation = LMO.Operation.newBuilder()
            .setEnv(env.build())
            .setRequirements(requirements)
            .setCommand(operation.getCommand())
            .addAllSlots(inputSlots)
            .addAllSlots(outputSlots)
            .setName(operation.getName())
            .setStdout(LMO.Operation.StdSlotDesc.newBuilder()
                .setName("/dev/" + Slot.STDOUT_SUFFIX).setChannelId(stdoutChannelId).build())
            .setStderr(LMO.Operation.StdSlotDesc.newBuilder()
                .setName("/dev/" + Slot.STDERR_SUFFIX).setChannelId(stderrChannelId).build())
            .build();

        return TaskDesc.newBuilder()
            .setId(taskId)
            .setOperation(taskOperation)
            .addAllSlotAssignments(slotToChannelAssignments)
            .build();
    }
}
