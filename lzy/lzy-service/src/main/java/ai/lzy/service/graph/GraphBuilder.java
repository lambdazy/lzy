package ai.lzy.service.graph;

import ai.lzy.model.slot.Slot;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
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

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.model.grpc.ProtoConverter.*;
import static ai.lzy.portal.grpc.ProtoConverter.*;
import static ai.lzy.portal.services.PortalService.PORTAL_SLOT_PREFIX;

class GraphBuilder {
    private static final Logger LOG = LogManager.getLogger(GraphBuilder.class);

    private final ExecutionDao executionDao;
    private final LzyChannelManagerPrivateBlockingStub channelManagerClient;

    public GraphBuilder(ExecutionDao executionDao, LzyChannelManagerPrivateBlockingStub channelManagerClient) {
        this.executionDao = executionDao;
        this.channelManagerClient = channelManagerClient;
    }

    public void build(GraphExecutionState state, LzyPortalGrpc.LzyPortalBlockingStub portalClient) {
        var userId = state.getUserId();
        var workflowName = state.getWorkflowName();
        var executionId = state.getExecutionId();
        var dataFlow = state.getDataFlowGraph().getDataflow();
        var slot2description = state.getDescriptions().stream()
            .collect(Collectors.toMap(LWF.DataDescription::getStorageUri, Function.identity()));

        Map<String, String> slot2channelId;
        try {
            slot2channelId = createChannelsForDataFlow(userId, workflowName, executionId,
                dataFlow, slot2description, portalClient, state);
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
            tasks = buildTasksWithZone(userId, workflowName, executionId, state.getZone(), state.getOperations(),
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

    private Map<String, String> createChannelsForDataFlow(String userId, String workflowName, String executionId,
                                                          List<DataFlowGraph.Data> dataFlow,
                                                          Map<String, LWF.DataDescription> slot2dataDescription,
                                                          LzyPortalGrpc.LzyPortalBlockingStub portalClient,
                                                          GraphExecutionState state)
    {
        var slotName2channelId = new HashMap<String, String>();
        var partitionBySupplier = dataFlow.stream().collect(Collectors.partitioningBy(data -> data.supplier() != null));
        var fromOutput = partitionBySupplier.get(true);
        var fromPortal = partitionBySupplier.get(false);

        LMST.StorageConfig storageConfig;
        try {
            storageConfig = withRetries(LOG, () -> executionDao.getStorageConfig(executionId));
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
                .create(makeCreateChannelCommand(userId, workflowName, executionId, "channel_" + slotUri))
                .getChannelId();
            var portalInputSlotName = PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
            var dataDescription = slot2dataDescription.get(slotUri);

            inputSlotNames.add(portalInputSlotName);
            portalSlotToOpen.add(makePortalInputSlot(slotUri, portalInputSlotName, channelId, storageConfig));

            slotName2channelId.put(data.supplier(), channelId);
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }
        }
        state.setPortalInputSlots(inputSlotNames);

        for (var data : fromPortal) {
            var slotUri = data.slotUri();
            var portalOutputSlotName = PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
            var channelId = channelManagerClient
                .create(makeCreateChannelCommand(userId, workflowName, executionId, "portal_channel_" + slotUri))
                .getChannelId();

            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }

            portalSlotToOpen.add(makePortalOutputSlot(slotUri, portalOutputSlotName, channelId, storageConfig));
        }

        //noinspection ResultOfMethodCallIgnored
        portalClient.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder().addAllSlots(portalSlotToOpen).build());
        return slotName2channelId;
    }

    private List<TaskDesc> buildTasksWithZone(String userId, String workflowName, String executionId, String zoneName,
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
                makeCreateChannelCommand(userId, workflowName, executionId, channelNameForStdoutSlot)).getChannelId();

            var channelNameForStderrSlot = "channel_" + taskId + ":" + Slot.STDERR_SUFFIX;
            var stderrChannelId = channelManagerClient.create(
                makeCreateChannelCommand(userId, workflowName, executionId, channelNameForStderrSlot)).getChannelId();

            tasks.add(buildTaskWithZone(taskId, operation, zoneName, stdoutChannelId, stderrChannelId, slot2Channel,
                slot2description, portalClient, operation.getEnvMap()));
        }

        return tasks;
    }

    private TaskDesc buildTaskWithZone(String taskId, LWF.Operation operation,
                                       String zoneName, String stdoutChannelId, String stderrChannelId,
                                       Map<String, String> slot2Channel,
                                       Map<String, LWF.DataDescription> slot2description,
                                       LzyPortalGrpc.LzyPortalBlockingStub portalClient,
                                       Map<String, String> envMap)
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

        var stdoutPortalSlotName = PORTAL_SLOT_PREFIX + "_" + taskId + ":" + Slot.STDOUT_SUFFIX;
        var stderrPortalSlotName = PORTAL_SLOT_PREFIX + "_" + taskId + ":" + Slot.STDERR_SUFFIX;

        //noinspection ResultOfMethodCallIgnored
        portalClient.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(makePortalInputStdoutSlot(taskId, stdoutPortalSlotName, stdoutChannelId))
            .addSlots(makePortalInputStderrSlot(taskId, stderrPortalSlotName, stderrChannelId))
            .build());

        var requirements = LMO.Requirements.newBuilder()
            .setZone(zoneName).setPoolLabel(operation.getPoolSpecName()).build();
        var env = LME.EnvSpec.newBuilder();

        env.setDockerImage(operation.getDockerImage());

        if (operation.hasDockerCredentials()) {
            env.setDockerCredentials(LME.DockerCredentials.newBuilder()
                .setUsername(operation.getDockerCredentials().getUsername())
                .setPassword(operation.getDockerCredentials().getPassword())
                .setRegistryName(operation.getDockerCredentials().getRegistryName())
                .build());
        }

        var policy = switch (operation.getDockerPullPolicy()) {
            case ALWAYS -> LME.DockerPullPolicy.ALWAYS;
            case IF_NOT_EXISTS -> LME.DockerPullPolicy.IF_NOT_EXISTS;
            case UNSPECIFIED -> LME.DockerPullPolicy.IF_NOT_EXISTS;  // default
            case UNRECOGNIZED -> throw Status.INVALID_ARGUMENT
                .withDescription("Wrong docker pull policy")
                .asRuntimeException();
        };

        env.setDockerPullPolicy(policy);

        if (operation.hasPython()) {
            env.setPyenv(
                LME.PythonEnv.newBuilder()
                    .setYaml(operation.getPython().getYaml())
                    .addAllLocalModules(
                        operation.getPython().getLocalModulesList().stream()
                            .map(module -> LME.LocalModule.newBuilder()
                                .setName(module.getName())
                                .setUri(module.getUrl())
                                .build()
                            ).toList()
                    ).build()
            );
        } else {
            env.setProcessEnv(LME.ProcessEnv.newBuilder().build());
        }

        env.putAllEnv(envMap);

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
