package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.slot.Slot;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.graph.DataFlowGraph;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.model.grpc.ProtoConverter.*;
import static ai.lzy.portal.grpc.ProtoConverter.*;
import static ai.lzy.portal.services.PortalService.PORTAL_SLOT_PREFIX;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.v1.portal.LzyPortalGrpc.newBlockingStub;

public class BuildGraph implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String userId;
    private final String wfName;
    private final String execId;

    private final String poolZone;
    private final String idempotencyKey;
    private final RenewableJwt internalUserCredentials;
    private final KafkaConfig kafkaConfig;
    private final DataFlowGraph dataFlowGraph;
    private final List<LWF.Operation> operations;
    private final Map<String, LWF.DataDescription> slot2description;

    private final List<String> portalInputSlotNames;
    private final LzyChannelManagerPrivateBlockingStub channelsClient;

    private final GraphExecuteRequest.Builder builder;
    private final Function<StatusRuntimeException, StepResult> failAction;

    private final Logger log;
    private final String logPrefix;

    private LzyPortalBlockingStub portalClient;

    public BuildGraph(ExecutionDao execDao, String userId, String wfName, String execId, String poolsZone,
                      RenewableJwt internalUserCredentials, KafkaConfig kafkaConfig, DataFlowGraph dataFlowGraph,
                      List<LWF.Operation> operations, Map<String, LWF.DataDescription> slot2description,
                      List<String> portalInputSlotNames, GraphExecuteRequest.Builder builder,
                      @Nullable String idempotencyKey, LzyChannelManagerPrivateBlockingStub channelsClient,
                      Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.userId = userId;
        this.wfName = wfName;
        this.execId = execId;
        this.poolZone = poolsZone;
        this.internalUserCredentials = internalUserCredentials;
        this.kafkaConfig = kafkaConfig;
        this.dataFlowGraph = dataFlowGraph;
        this.operations = operations;
        this.slot2description = slot2description;
        this.portalInputSlotNames = portalInputSlotNames;
        this.builder = builder;
        this.idempotencyKey = idempotencyKey;
        this.channelsClient = channelsClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Building graph...", logPrefix);

        String portalVmAddress;
        try {
            portalVmAddress = withRetries(log, () -> execDao.getPortalVmAddress(execId));
        } catch (Exception e) {
            log.error("{} Cannot get portal address: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot build graph").asRuntimeException());
        }

        portalClient = newBlockingClient(newBlockingStub(newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME)),
            APP, () -> internalUserCredentials.get().token());

        log.debug("{} Create channels for tasks slots...", logPrefix);

        Map<String, String> slot2channelId;
        try {
            slot2channelId = createChannelsForDataFlow(slot2description);
        } catch (StatusRuntimeException sre) {
            log.error("{} Cannot assign slots to channels: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        } catch (Exception e) {
            log.error("{} Cannot assign slots to channels: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot build graph").asRuntimeException());
        }

        final KafkaTopicDesc desc;
        try {
            desc = withRetries(log, () -> execDao.getKafkaTopicDesc(execId, null));
        } catch (Exception e) {
            log.error("{} Cannot get topic description from db: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot get kafka topic").asRuntimeException());
        }

        final LMO.KafkaTopicDescription kafkaTopicDescription;

        if (desc != null) {
            kafkaTopicDescription = LMO.KafkaTopicDescription.newBuilder()
                .setTopic(desc.topicName())
                .setUsername(desc.username())
                .setPassword(desc.password())
                .addAllBootstrapServers(kafkaConfig.getBootstrapServers())
                .build();
        } else {
            kafkaTopicDescription = null;
        }

        log.debug("{} Building tasks...", logPrefix);

        List<GraphExecutor.TaskDesc> tasks;
        try {
            tasks = buildTasksWithZone(slot2channelId, slot2description, poolZone, operations, kafkaTopicDescription);
        } catch (StatusRuntimeException sre) {
            log.error("{} Cannot build tasks of graph: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        } catch (Exception e) {
            log.error("{} Cannot build tasks of graph: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot build graph").asRuntimeException());
        }

        var channelIds = new HashSet<>(slot2channelId.values());
        var channelsDescriptions = channelIds.stream()
            .map(id -> GraphExecutor.ChannelDesc
                .newBuilder()
                .setId(id)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build())
            .toList();

        builder.addAllTasks(tasks);
        builder.addAllChannels(channelsDescriptions);

        return StepResult.CONTINUE;
    }

    private Map<String, String> createChannelsForDataFlow(Map<String, LWF.DataDescription> slot2dataDescription) {
        var slotName2channelId = new HashMap<String, String>();
        var partitionBySupplier = dataFlowGraph.getDataflow().stream().collect(
            Collectors.partitioningBy(data -> data.supplier() != null));
        var fromOutput = partitionBySupplier.get(true);
        var fromPortal = partitionBySupplier.get(false);

        LMST.StorageConfig storageConfig;
        try {
            storageConfig = withRetries(log, () -> execDao.getStorageConfig(execId));
        } catch (Exception e) {
            log.error("{} Cannot get storage config for execution with id='{}': {}", logPrefix, execId,
                e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e);
        }

        var portalSlotToOpen = new ArrayList<LzyPortal.PortalSlotDesc>();

        log.debug("{} Create channels for data from other graph tasks...", logPrefix);
        for (var data : fromOutput) {
            var slotUri = data.slotUri();
            var idempotentChannelsClient = (idempotencyKey == null) ? channelsClient :
                withIdempotencyKey(channelsClient, idempotencyKey + "_" + slotUri);
            var channelId = idempotentChannelsClient.create(makeCreateChannelCommand(userId, wfName, execId,
                "channel_" + slotUri)).getChannelId();
            var portalInputSlotName = PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
            var dataDescription = slot2dataDescription.get(slotUri);

            portalInputSlotNames.add(portalInputSlotName);
            portalSlotToOpen.add(makePortalInputSlot(slotUri, dataDescription.hasDataScheme() ?
                dataDescription.getDataScheme() : null, portalInputSlotName, channelId, storageConfig));

            slotName2channelId.put(data.supplier(), channelId);
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }
        }

        log.debug("{} Create channels for data from portal...", logPrefix);
        for (var data : fromPortal) {
            var slotUri = data.slotUri();
            var channelId = channelsClient.create(makeCreateChannelCommand(userId, wfName, execId,
                "portal_channel_" + slotUri + "_" + UUID.randomUUID())).getChannelId();
            var portalOutputSlotName = PORTAL_SLOT_PREFIX + "_" + UUID.randomUUID();
            var dataDescription = slot2dataDescription.get(slotUri);

            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }

            portalSlotToOpen.add(makePortalOutputSlot(slotUri, dataDescription.hasDataScheme() ?
                dataDescription.getDataScheme() : null, portalOutputSlotName, channelId, storageConfig));
        }

        log.debug("{} Open created portal slots for tasks...", logPrefix);
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(portalClient, idempotencyKey + "_open_tasks_data_slots").openSlots(
            LzyPortalApi.OpenSlotsRequest.newBuilder().addAllSlots(portalSlotToOpen).build());
        return slotName2channelId;
    }

    private List<GraphExecutor.TaskDesc> buildTasksWithZone(Map<String, String> slot2Channel,
                                                            Map<String, LWF.DataDescription> slot2description,
                                                            String zone, List<LWF.Operation> operations,
                                                            @Nullable LMO.KafkaTopicDescription kafkaTopic)
    {
        int count = operations.size();
        var taskBuilders = new ArrayList<GraphExecutor.TaskDesc.Builder>(count);
        var stdoutChannelIds = new ArrayList<String>(count);
        var stderrChannelIds = new ArrayList<String>(count);

        log.debug("{} Create channels for stdout/stderr tasks slots...", logPrefix);
        for (int i = 0; i < count; i++) {
            var taskBuilder = GraphExecutor.TaskDesc.newBuilder().setId(UUID.randomUUID().toString());

            if (!this.kafkaConfig.isEnabled()) {
                var channelNameForStdoutSlot = "channel_" + taskBuilder.getId() + ":" + Slot.STDOUT_SUFFIX;
                stdoutChannelIds.add(channelsClient.create(makeCreateChannelCommand(userId,
                    wfName, execId, channelNameForStdoutSlot)).getChannelId());

                var channelNameForStderrSlot = "channel_" + taskBuilder.getId() + ":" + Slot.STDERR_SUFFIX;
                stderrChannelIds.add(channelsClient.create(makeCreateChannelCommand(userId,
                    wfName, execId, channelNameForStderrSlot)).getChannelId());
            }

            taskBuilders.add(taskBuilder);
        }

        log.debug("{} Create stdout/stderr portal slots for tasks and build tasks...", logPrefix);
        for (int i = 0; i < count; i++) {
            buildTaskWithZone(
                taskBuilders.get(i),
                operations.get(i),
                zone,
                this.kafkaConfig.isEnabled() ? null : stdoutChannelIds.get(i),
                this.kafkaConfig.isEnabled() ? null : stderrChannelIds.get(i),
                slot2Channel,
                slot2description,
                kafkaTopic);
        }

        return taskBuilders.stream().map(GraphExecutor.TaskDesc.Builder::build).toList();
    }

    private void buildTaskWithZone(GraphExecutor.TaskDesc.Builder taskBuilder, LWF.Operation operation,
                                   String zoneName, String stdoutChannelId, String stderrChannelId,
                                   Map<String, String> slot2Channel,
                                   Map<String, LWF.DataDescription> slot2description,
                                   @Nullable LMO.KafkaTopicDescription kafkaTopic)
    {
        var inputSlots = new ArrayList<LMS.Slot>();
        var outputSlots = new ArrayList<LMS.Slot>();
        var slotToChannelAssignments = new ArrayList<GraphExecutor.SlotToChannelAssignment>();

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

                slotToChannelAssignments.add(GraphExecutor.SlotToChannelAssignment.newBuilder()
                    .setSlotName(slotName)
                    .setChannelId(slot2Channel.get(slotName))
                    .build());
            }
        };

        slotsDescriptionsConsumer.accept(operation.getInputSlotsList(), true);
        slotsDescriptionsConsumer.accept(operation.getOutputSlotsList(), false);

        var stdoutPortalSlotName = PORTAL_SLOT_PREFIX + "_" + taskBuilder.getId() + ":" + Slot.STDOUT_SUFFIX;
        var stderrPortalSlotName = PORTAL_SLOT_PREFIX + "_" + taskBuilder.getId() + ":" + Slot.STDERR_SUFFIX;

        var builder = LzyPortalApi.OpenSlotsRequest.newBuilder();
        if (stdoutChannelId != null) {
            builder.addSlots(makePortalInputStdoutSlot(taskBuilder.getId(), stdoutPortalSlotName, stdoutChannelId));
        }

        if (stderrChannelId != null) {
            builder.addSlots(makePortalInputStderrSlot(taskBuilder.getId(), stderrPortalSlotName, stderrChannelId));
        }

        if (stdoutChannelId != null || stderrChannelId != null) {
            //noinspection ResultOfMethodCallIgnored
            portalClient.openSlots(builder.build());
        }

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

        env.putAllEnv(operation.getEnvMap());

        var taskOperation = LMO.Operation.newBuilder()
            .setEnv(env.build())
            .setRequirements(requirements)
            .setCommand(operation.getCommand())
            .addAllSlots(inputSlots)
            .addAllSlots(outputSlots)
            .setName(operation.getName());

        if (kafkaTopic != null) {
            taskOperation.setKafkaTopic(kafkaTopic);
        }

        if (stdoutChannelId != null) {
            taskOperation.setStdout(LMO.Operation.StdSlotDesc.newBuilder()
                .setName("/dev/" + Slot.STDOUT_SUFFIX).setChannelId(stdoutChannelId).build());
        }

        if (stderrChannelId != null) {
            taskOperation.setStderr(LMO.Operation.StdSlotDesc.newBuilder()
                .setName("/dev/" + Slot.STDERR_SUFFIX).setChannelId(stderrChannelId).build());
        }

        taskBuilder.setOperation(taskOperation).addAllSlotAssignments(slotToChannelAssignments).build();
    }
}
