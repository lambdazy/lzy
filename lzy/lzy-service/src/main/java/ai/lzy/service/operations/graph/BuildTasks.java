package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import io.grpc.Status;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.lzy.model.grpc.ProtoConverter.*;
import static ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;

public final class BuildTasks extends ExecuteGraphContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final KafkaConfig kafkaCfg;
    private final KafkaTopicDesc kafkaTopicDesc;
    private final Map<String, LWF.DataDescription> slotUri2dataDescription;

    public BuildTasks(ExecutionStepContext stepCtx, ExecuteGraphState state,
                      KafkaConfig kafkaCfg, KafkaTopicDesc kafkaTopicDesc)
    {
        super(stepCtx, state);
        this.kafkaCfg = kafkaCfg;
        this.kafkaTopicDesc = kafkaTopicDesc;
        this.slotUri2dataDescription = request().getDataDescriptionsList().stream().collect(Collectors.toMap(
            LWF.DataDescription::getStorageUri, Function.identity()));
    }

    @Override
    public StepResult get() {
        if (tasks() != null) {
            log().debug("{} Tasks already built, skip test...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Building graph tasks: { wfName: {}, execId: {} }", logPrefix(), wfName(), execId());

        final List<GraphExecutor.TaskDesc> tasks;
        try {
            tasks = operationsToExecute().stream().map(this::buildTask).toList();
        } catch (IllegalArgumentException iae) {
            log().error("{} Cannot build graph task: {}", logPrefix(), iae.getMessage(), iae);
            return failAction().apply(Status.INVALID_ARGUMENT.withDescription(iae.getMessage()).asRuntimeException());
        }

        log().debug("{} Save data about graph tasks in dao...", logPrefix());
        setTasks(tasks);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save data about graph tasks in dao", Status.INTERNAL
                .withDescription("Cannot build graph tasks").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }

    private GraphExecutor.TaskDesc buildTask(LWF.Operation operation) throws IllegalArgumentException {
        var taskBuilder = GraphExecutor.TaskDesc.newBuilder().setId(idGenerator().generate());

        var env = buildEnv(operation);
        var requirements = LMO.Requirements.newBuilder().setZone(vmPoolZone())
            .setPoolLabel(operation.getPoolSpecName()).build();

        var kafkaTopicDescription = LMO.KafkaTopicDescription.newBuilder()
            .setTopic(kafkaTopicDesc.topicName())
            .setUsername(kafkaTopicDesc.username())
            .setPassword(kafkaTopicDesc.password())
            .addAllBootstrapServers(kafkaCfg.getBootstrapServers())
            .build();

        var inputSlots = buildSlots(operation.getInputSlotsList(), true);
        var outputSlots = buildSlots(operation.getOutputSlotsList(), false);

        var taskOperation = LMO.Operation.newBuilder()
            .setName(operation.getName())
            .setEnv(env)
            .setRequirements(requirements)
            .setCommand(operation.getCommand())
            .setKafkaTopic(kafkaTopicDescription)
            .addAllSlots(inputSlots)
            .addAllSlots(outputSlots);

        var slotsAssignments = buildSlotsAssignments(operation);

        return taskBuilder.setOperation(taskOperation).addAllSlotAssignments(slotsAssignments).build();
    }

    private List<LMS.Slot> buildSlots(Collection<SlotDescription> slotsDescriptions, boolean isInput) {
        return slotsDescriptions.stream().map(slotDescription -> {
            var uri = slotDescription.getStorageUri();
            var slotName = slotDescription.getPath();
            var description = slotUri2dataDescription.get(uri);

            var hasDataScheme = description != null && description.hasDataScheme();

            if (isInput) {
                return hasDataScheme ? buildFileInputSlot(slotName, description.getDataScheme()) :
                    buildFileInputPlainContentSlot(slotName);
            } else {
                return hasDataScheme ? buildFileOutputSlot(slotName, description.getDataScheme()) :
                    buildFileOutputPlainContentSlot(slotName);
            }
        }).toList();
    }

    private List<GraphExecutor.SlotToChannelAssignment> buildSlotsAssignments(LWF.Operation operation) {
        return Stream.concat(operation.getInputSlotsList().stream(), operation.getOutputSlotsList().stream())
            .map(slotDescription -> GraphExecutor.SlotToChannelAssignment.newBuilder()
                .setSlotName(slotDescription.getPath()).setChannelId(channels().get(slotDescription.getStorageUri()))
                .build()
            ).toList();
    }

    private LME.EnvSpec buildEnv(LWF.Operation operation) throws IllegalArgumentException {
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
            case UNRECOGNIZED -> throw new IllegalArgumentException("Wrong docker pull policy");
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
        return env.build();
    }
}
