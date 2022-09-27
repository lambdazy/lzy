package ai.lzy.service;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.*;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.*;
import ai.lzy.v1.common.LME.AuxEnv;
import ai.lzy.v1.common.LME.BaseEnv;
import ai.lzy.v1.common.LME.LocalModule;
import ai.lzy.v1.common.LME.PythonEnv;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc;
import ai.lzy.v1.portal.LzyPortalApi.OpenSlotsRequest;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.DataDescription;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.v1.VmPoolServiceApi.GetVmPoolsRequest;
import static ai.lzy.v1.VmPoolServiceApi.VmPoolSpec;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final Duration allocationTimeout;

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;
    private final String channelManagerAddress;
    private final JwtCredentials internalUserCredentials;

    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final LzyServiceStorage storage;

    private final ManagedChannel allocatorServiceChannel;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    private final ManagedChannel operationServiceChannel;
    private final OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceClient;

    private final ManagedChannel storageServiceChannel;
    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;

    private final ManagedChannel channelManagerChannel;
    private final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManagerClient;

    private final ManagedChannel iamChannel;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    private final ManagedChannel graphExecutorChannel = null;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient = null;

    @SuppressWarnings("checkstyle:CommentsIndentation")
    public LzyService(LzyServiceConfig config, LzyServiceStorage storage,
                      WorkflowDao workflowDao, ExecutionDao executionDao)
    {
        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        startupPortalConfig = config.getPortal();
        allocationTimeout = config.getWaitAllocationTimeout();
        channelManagerAddress = config.getChannelManagerAddress();
        internalUserCredentials = config.getIam().createCredentials();

        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        var allocatorAddress = HostAndPort.fromString(config.getAllocatorAddress());

        allocatorServiceChannel = ChannelBuilder.forAddress(allocatorAddress)
            .usePlaintext()
            .enableRetry(AllocatorGrpc.SERVICE_NAME)
            .build();
        allocatorClient = AllocatorGrpc.newBlockingStub(allocatorServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        vmPoolClient = VmPoolServiceGrpc.newBlockingStub(allocatorServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        operationServiceChannel = ChannelBuilder.forAddress(allocatorAddress)
            .usePlaintext()
            .enableRetry(OperationServiceApiGrpc.SERVICE_NAME)
            .build();
        operationServiceClient = OperationServiceApiGrpc.newBlockingStub(operationServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        storageServiceChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getStorage().getAddress()))
            .usePlaintext()
            .enableRetry(LzyStorageServiceGrpc.SERVICE_NAME)
            .build();
        storageServiceClient = LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        channelManagerChannel = ChannelBuilder.forAddress(HostAndPort.fromString(config.getChannelManagerAddress()))
            .usePlaintext()
            .enableRetry(LzyChannelManagerGrpc.SERVICE_NAME)
            .build();
        channelManagerClient = LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        iamChannel = ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();

        subjectClient = new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
        abClient = new AccessBindingServiceGrpcClient(iamChannel, config.getIam()::createCredentials);

//        graphExecutorChannel = ChannelBuilder.forAddress("").build();
//        graphExecutorClient = GraphExecutorGrpc.newBlockingStub(graphExecutorChannel)
//            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
//                internalUserCredentials::token));
    }

    @SuppressWarnings("checkstyle:CommentsIndentation")
    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
        channelManagerChannel.shutdown();
//        graphExecutorChannel.shutdown();
        iamChannel.shutdown();
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();
        var workflowName = request.getWorkflowName();
        var executionId = workflowName + "_" + UUID.randomUUID();

        boolean internalSnapshotStorage = !request.hasSnapshotStorage();
        String storageType;
        LMS3.S3Locator storageData;

        BiConsumer<Status, String> replyError = (status, descr) -> {
            LOG.error("[createWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asRuntimeException());
        };

        if (internalSnapshotStorage) {
            storageType = "internal";
            try {
                storageData = createTempStorageBucket(userId);
            } catch (StatusRuntimeException e) {
                replyError.accept(e.getStatus(), "Cannot create internal storage");
                return;
            }
            if (storageData == null) {
                replyError.accept(Status.INTERNAL, "Cannot create internal storage");
                return;
            }
        } else {
            storageType = "user";
            // TODO: ssokolvyak -- move to validator
            if (request.getSnapshotStorage().getEndpointCase() == LMS3.S3Locator.EndpointCase.ENDPOINT_NOT_SET) {
                replyError.accept(Status.INVALID_ARGUMENT, "Snapshot storage not set");
                return;
            }
            storageData = request.getSnapshotStorage();
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.create(executionId, userId, workflowName, storageType, storageData, null));
        } catch (AlreadyExistsException e) {
            if (internalSnapshotStorage) {
                safeDeleteTempStorageBucket(storageData.getBucket());
            }
            replyError.accept(Status.ALREADY_EXISTS, "Cannot create new execution with " +
                "{ workflowName: '%s', userId: '%s' }: %s".formatted(workflowName, userId, e.getMessage()));
            return;
        } catch (Exception e) {
            if (internalSnapshotStorage) {
                safeDeleteTempStorageBucket(storageData.getBucket());
            }
            replyError.accept(Status.INTERNAL, "Cannot create new execution with " +
                "{ workflowName: '%s', userId: '%s' }: %s".formatted(workflowName, userId, e.getMessage()));
            return;
        }

        if (startPortal(workflowName, executionId, userId, response)) {
            LOG.info("Workflow successfully started...");

            var result = CreateWorkflowResponse.newBuilder().setExecutionId(executionId);
            if (internalSnapshotStorage) {
                result.setInternalSnapshotStorage(storageData);
            }
            response.onNext(result.build());
            response.onCompleted();
        }
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[attachWorkflow], userId={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[attachWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asRuntimeException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        try {
            boolean result = withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.doesActiveExecutionExists(userId, request.getWorkflowName(), request.getExecutionId()));

            if (result) {
                LOG.info("[attachWorkflow] workflow '{}/{}' successfully attached.",
                    request.getWorkflowName(), request.getExecutionId());

                response.onNext(AttachWorkflowResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                replyError.accept(Status.NOT_FOUND, "");
            }
        } catch (Exception e) {
            LOG.error("[attachWorkflow] Got Exception: " + e.getMessage(), e);
            replyError.accept(Status.INTERNAL, "Cannot retrieve data about workflow");
        }
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LOG.info("[finishWorkflow], uid={}, request={}.", userId, JsonUtils.printSingleLine(request));

        BiConsumer<io.grpc.Status, String> replyError = (status, descr) -> {
            LOG.error("[finishWorkflow], fail: status={}, msg={}.", status, descr);
            response.onError(status.withDescription(descr).asException());
        };

        if (StringUtils.isEmpty(request.getWorkflowName()) || StringUtils.isEmpty(request.getExecutionId())) {
            replyError.accept(Status.INVALID_ARGUMENT, "Empty 'workflowName' or 'executionId'");
            return;
        }

        // final String[] bucket = {null};
        // bucket[0] = retrieve from db
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    workflowDao.updateFinishData(request.getWorkflowName(), request.getExecutionId(),
                        Timestamp.from(Instant.now()), request.getReason(), transaction);
                    workflowDao.updateActiveExecution(userId, request.getWorkflowName(), request.getExecutionId(),
                        null);

                    transaction.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("[finishWorkflow], fail: {}.", e.getMessage(), e);
            replyError.accept(Status.INTERNAL, "Cannot finish workflow with name '" +
                request.getWorkflowName() + "': " + e.getMessage());
            return;
        }

        response.onNext(FinishWorkflowResponse.getDefaultInstance());
        response.onCompleted();

        // TODO: add TTL instead of implicit delete
        // safeDeleteTempStorageBucket(bucket[0]);
    }

    private boolean startPortal(String workflowName, String executionId, String userId,
                                StreamObserver<CreateWorkflowResponse> response)
    {
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.updateStatus(executionId, PortalStatus.CREATING_STD_CHANNELS));

            String[] portalChannelIds = createPortalStdChannels(executionId);
            var stdoutChannelId = portalChannelIds[0];
            var stderrChannelId = portalChannelIds[1];

            withRetries(defaultRetryPolicy(), LOG, () ->
                workflowDao.updateStdChannelIds(executionId, stdoutChannelId, stderrChannelId));

            var sessionId = createSession(userId);

            withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.updateAllocatorSession(executionId, sessionId));

            var startAllocationTime = Instant.now();
            var operation = startAllocation(workflowName, sessionId, executionId, stdoutChannelId, stderrChannelId);
            var opId = operation.getId();

            VmAllocatorApi.AllocateMetadata allocateMetadata;
            try {
                allocateMetadata = operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
            } catch (InvalidProtocolBufferException e) {
                response.onError(Status.INTERNAL
                    .withDescription("Invalid allocate operation metadata: VM id missed. Operation id: " + opId)
                    .asRuntimeException());
                return false;
            }
            var vmId = allocateMetadata.getVmId();

            withRetries(defaultRetryPolicy(), LOG,
                () -> workflowDao.updateAllocateOperationData(executionId, opId, vmId));

            VmAllocatorApi.AllocateResponse
                allocateResponse = waitAllocation(startAllocationTime.plus(allocationTimeout), opId);
            if (allocateResponse == null) {
                LOG.error("Cannot wait allocate operation response. Operation id: " + opId);
                response.onError(Status.DEADLINE_EXCEEDED.withDescription("Allocation timeout").asRuntimeException());
                return false;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> workflowDao.updateAllocatedVmAddress(executionId,
                allocateResponse.getMetadataOrDefault(AllocatorAgent.VM_IP_ADDRESS, null)));

        } catch (Exception e) {
            response.onError(Status.INTERNAL.withDescription("Cannot save execution data about portal")
                .asRuntimeException());
            return false;
        }
        return true;
    }

    @SuppressWarnings("checkstyle:CommentsIndentation")
    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> response) {
        // this method is insensitive to order of operations in Graph::getOperationList()

        var executionId = request.getExecutionId();
        var graph = request.getGraph();
        var slot2description = graph.getDataDescriptionsList().parallelStream()
            .collect(Collectors.toConcurrentMap(DataDescription::getStorageUri, Function.identity()));
        var zoneName = determineZone(graph.getOperationsList()
            .parallelStream()
            .map(LWF.Operation::getPoolSpecName)
            .toList());

        var inputSlots = new ArrayList<SlotDescription>();
        var outputSlots = new ArrayList<SlotDescription>();
        for (var operation : graph.getOperationsList()) {
            inputSlots.addAll(operation.getInputSlotsList());
            outputSlots.addAll(operation.getOutputSlotsList());
        }

        String workflowName;
        try {
            workflowName = withRetries(LOG, () -> workflowDao.getWorkflowNameBy(executionId));
        } catch (Exception e) {
            response.onError(e);
            return;
        }

        // slot name to channel id
        Map<String, String> slots2channels = assignSlotsToChannels(executionId, workflowName, inputSlots, outputSlots);

        List<TaskDesc> tasks = buildTasksWithZone(zoneName, executionId, graph.getOperationsList(),
            slots2channels, slot2description);
        List<GraphExecutor.ChannelDesc> channels = buildChannelDescriptions(slots2channels.values());

        GraphExecutorApi.GraphExecuteResponse executeResponse = GraphExecutorApi.GraphExecuteResponse.newBuilder()
            .setStatus(GraphExecutor.GraphExecutionStatus.newBuilder().setGraphId("1").build())
            .build();

//        graphExecutorClient.execute(GraphExecutorApi.GraphExecuteRequest.newBuilder()
//                .setWorkflowId(executionId)
//                .setWorkflowName(workflowName)
//                .setParentGraphId(graph.getParentGraphId())
//                .addAllTasks(tasks)
//                .addAllChannels(channels)
//                .build());

        response.onNext(ExecuteGraphResponse.newBuilder().setGraphId(executeResponse.getStatus().getGraphId()).build());
        response.onCompleted();
    }

    private String determineZone(Collection<String> requiredPoolLabels) {
        List<VmPoolSpec> availablePools = vmPoolClient.getVmPools(GetVmPoolsRequest.newBuilder()
            .setWithSystemPools(false)
            .setWithUserPools(true)
            .build()).getUserPoolsList();

        var requiredPools = new ArrayList<VmPoolSpec>(requiredPoolLabels.size());
        for (var targetLabel : requiredPoolLabels) {
            var found = availablePools.parallelStream()
                .filter(pool -> targetLabel.contentEquals(pool.getLabel())).findAny();
            if (found.isEmpty()) {
                throw new IllegalArgumentException("Cannot find pool with label: " + targetLabel);
            }
            requiredPools.add(found.get());
        }

        Set<String> commonZones = requiredPools.stream().collect(
            () -> new HashSet<>(requiredPools.get(0).getZonesList()),
            (common, spec) -> common.retainAll(spec.getZonesList()), Collection::addAll);

        return commonZones.stream().findAny().orElseThrow(() ->
            new IllegalArgumentException("Cannot find zone which has all required pools"));
    }

    private List<GraphExecutor.ChannelDesc> buildChannelDescriptions(Collection<String> channelIds) {
        return channelIds.parallelStream()
            .map(id -> GraphExecutor.ChannelDesc
                .newBuilder()
                .setId(id)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build())
            .toList();
    }

    private TaskDesc buildTaskWithZone(String executionId, String taskId, LWF.Operation operation,
                                       String zoneName, String stdoutChannelId, String stderrChannelId,
                                       Map<String, String> slot2Channel, Map<String, DataDescription> slot2description)
    {
        var env = LME.EnvSpec.newBuilder();
        if (!operation.getDockerImage().isBlank()) {
            env.setBaseEnv(BaseEnv.newBuilder().setName(operation.getDockerImage()).build());
        }
        if (operation.hasPython()) {
            env.setAuxEnv(AuxEnv.newBuilder().setPyenv(
                    PythonEnv.newBuilder()
                        .setYaml(operation.getPython().getYaml())
                        .addAllLocalModules(
                            operation.getPython().getLocalModulesList().parallelStream()
                                .map(module -> LocalModule.newBuilder()
                                    .setName(module.getName())
                                    .setUri(module.getUrl())
                                    .build()
                                ).toList()
                        ).build())
                .build());
        }

        var slotToChannelAssignments = new ArrayList<GraphExecutor.SlotToChannelAssignment>();
        var inputSlots = new ArrayList<LMS.Slot>();
        var outputSlots = new ArrayList<LMS.Slot>();

        for (var slotDescription : operation.getInputSlotsList()) {
            var uri = slotDescription.getStorageUri();
            var description = slot2description.get(uri);

            if (description == null) {
                throw new RuntimeException("Cannot find data description for input slot: " +
                    slotDescription.getPath());
            }

            LMS.Slot.Builder slot = LMS.Slot.newBuilder()
                .setName(slotDescription.getPath())
                .setDirection(LMS.Slot.Direction.INPUT)
                .setMedia(LMS.Slot.Media.FILE);
            if (description.hasDataScheme()) {
                slot.setContentType(description.getDataScheme());
            }

            inputSlots.add(slot.build());
            slotToChannelAssignments.add(GraphExecutor.SlotToChannelAssignment.newBuilder()
                .setSlotName(slotDescription.getPath())
                .setChannelId(slot2Channel.get(slotDescription.getPath()))
                .build());
        }

        for (var slotDescription : operation.getOutputSlotsList()) {
            var uri = slotDescription.getStorageUri();
            var description = slot2description.get(uri);

            if (description == null) {
                throw new RuntimeException("Cannot find data description for output slot: " +
                    slotDescription.getPath());
            }

            LMS.Slot.Builder slot = LMS.Slot.newBuilder()
                .setName(slotDescription.getPath())
                .setDirection(LMS.Slot.Direction.OUTPUT)
                .setMedia(LMS.Slot.Media.FILE);
            if (description.hasDataScheme()) {
                slot.setContentType(description.getDataScheme());
            }

            outputSlots.add(slot.build());
            slotToChannelAssignments.add(GraphExecutor.SlotToChannelAssignment.newBuilder()
                .setSlotName(slotDescription.getPath())
                .setChannelId(slot2Channel.get(slotDescription.getPath()))
                .build());
        }

        var requirements = LMO.Requirements.newBuilder()
            .setZone(zoneName).setPoolLabel(operation.getPoolSpecName()).build();
        var stdoutSlotName = "/dev/stdout";
        var stderrSlotName = "/dev/stderr";

        var stdoutPortalSlotName = "/portal_%s:stdout".formatted(taskId);
        var stderrPortalSlotName = "/portal_%s:stderr".formatted(taskId);

        String portalAddress;
        try {
            portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddressFor(executionId));
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        var portalChannel = ChannelBuilder.forAddress(portalAddress).usePlaintext().build();
        LzyPortalGrpc.LzyPortalBlockingStub portalClient = LzyPortalGrpc.newBlockingStub(portalChannel);


        portalClient.openSlots(OpenSlotsRequest.newBuilder()
            .addSlots(PortalSlotDesc.newBuilder()
                .setSlot(LMS.Slot.newBuilder()
                    .setName(stdoutPortalSlotName)
                    .setMedia(LMS.Slot.Media.FILE)
                    .setDirection(LMS.Slot.Direction.INPUT)
                    .setContentType(LMD.DataScheme.newBuilder()
                        .setSchemeContent("text")
                        .setDataFormat("plain")
                        .build())
                    .build())
                .setChannelId(stdoutChannelId)
                .setStdout(PortalSlotDesc.StdOut.newBuilder()
                    .setTaskId(taskId)
                    .build())
                .build())
            .addSlots(PortalSlotDesc.newBuilder()
                .setSlot(LMS.Slot.newBuilder()
                    .setName(stderrPortalSlotName)
                    .setMedia(LMS.Slot.Media.FILE)
                    .setDirection(LMS.Slot.Direction.INPUT)
                    .setContentType(LMD.DataScheme.newBuilder()
                        .setSchemeContent("text")
                        .setDataFormat("plain")
                        .build())
                    .build())
                .setChannelId(stderrChannelId)
                .setStderr(PortalSlotDesc.StdErr.newBuilder()
                    .setTaskId(taskId)
                    .build())
                .build())
            .build());

        var taskOperation = LMO.Operation.newBuilder()
            .setEnv(env.build())
            .setRequirements(requirements)
            .setCommand(operation.getCommand())
            .addAllSlots(inputSlots)
            .addAllSlots(outputSlots)
            .setName(operation.getName())
            .setStdout(LMO.Operation.StdSlotDesc.newBuilder()
                .setName(stdoutSlotName).setChannelId(stdoutChannelId).build())
            .setStderr(LMO.Operation.StdSlotDesc.newBuilder()
                .setName(stderrSlotName).setChannelId(stderrChannelId).build())
            .build();

        return TaskDesc.newBuilder()
            .setId(taskId)
            .setOperation(taskOperation)
            .addAllSlotAssignments(slotToChannelAssignments)
            .build();
    }

    private List<TaskDesc> buildTasksWithZone(String executionId, String zoneName,
                                              Collection<LWF.Operation> operations,
                                              Map<String, String> slot2Channel,
                                              Map<String, DataDescription> slot2description)
    {
        var tasks = new ArrayList<TaskDesc>(operations.size());

        for (var operation : operations) {
            // TODO: ssokolvyak -- must not be generated here but passed in executeGraph request
            var taskId = UUID.randomUUID().toString();

            var stdoutChannelName = taskId + ":stdout";
            var stderrChannelName = taskId + ":stderr";
            var stdoutChannelId = createChannel(stdoutChannelName, executionId);
            var stderrChannelId = createChannel(stderrChannelName, executionId);

            tasks.add(buildTaskWithZone(executionId, taskId, operation, zoneName,
                stdoutChannelId, stderrChannelId, slot2Channel, slot2description));
        }

        return tasks;
    }

    private String createChannel(String name, String executionId) {
        return channelManagerClient.create(LCMS.ChannelCreateRequest.newBuilder()
            .setWorkflowId(executionId)
            .setChannelSpec(LCM.ChannelSpec.newBuilder()
                .setChannelName(name)
                .setContentType(LMD.DataScheme.newBuilder()
                    .setSchemeContent("text")
                    .setDataFormat("plain")
                    .build())
                .setDirect(LCM.DirectChannelType.getDefaultInstance())
                .build())
            .build()).getChannelId();
    }

    @Nullable
    private static String findFirstDuplicate(Collection<String> col) {
        var scanned = new HashSet<String>();
        for (var cur : col) {
            if (scanned.contains(cur)) {
                return cur;
            }
            scanned.add(cur);
        }
        return null;
    }

    // slot name to channel id
    private Map<String, String> assignSlotsToChannels(String executionId, String workflowName,
                                                      Collection<SlotDescription> inputSlotDescriptions,
                                                      Collection<SlotDescription> outputSlotDescriptions)
    {
        var slotsUriAsOutput = outputSlotDescriptions.parallelStream().map(SlotDescription::getStorageUri).toList();
        var duplicate = findFirstDuplicate(slotsUriAsOutput);

        if (duplicate != null) {
            LOG.error("Duplicate output slot URI { executionId: {}, slotUri: {}, workflowName: {}} ",
                executionId, duplicate, workflowName);
            throw new RuntimeException("Duplicate output slot uri: " + duplicate);
        }

        List<String> listOfAlreadyPresented;
        try {
            listOfAlreadyPresented = withRetries(LOG, () -> executionDao.whichSlotsUriPresented(slotsUriAsOutput));
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        if (!listOfAlreadyPresented.isEmpty()) {
            var alreadyUsedSlotUri = listOfAlreadyPresented.get(0);
            LOG.error("Output slot URI already used in execution { slotUri: {}, executionId: {}, workflowName: {} }",
                alreadyUsedSlotUri, executionId, workflowName);
            throw new RuntimeException("Already used slot uri: " + alreadyUsedSlotUri);
        }

        var channelsFromOutputSlots = slotsUriAsOutput.parallelStream()
            .collect(Collectors.toConcurrentMap(Function.identity(), slotUri ->
                channelManagerClient
                    .create(GrpcConverter.createChannelRequest(executionId, createChannelSpec("channel_" + slotUri)))
                    .getChannelId()));

        var inputSlotsNotAssignToOutput = inputSlotDescriptions.parallelStream()
            .map(SlotDescription::getStorageUri)
            .filter(slotUri -> !channelsFromOutputSlots.containsKey(slotUri))
            .toList();

        Set<String> foundOnPortal;
        try {
            foundOnPortal = withRetries(LOG, () -> executionDao.getSlotsUriStoredOnPortalBy(executionId));
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        var notFound = inputSlotsNotAssignToOutput.parallelStream()
            .filter(slotUri -> !foundOnPortal.contains(slotUri)).toList();

        if (!notFound.isEmpty()) {
            LOG.error("Input slot URI { slotUri: {} } is presented neither in output slot URIs " +
                "nor stored as already associated with portal", notFound.get(0));
            throw new RuntimeException("Unknown slot uri");
        }

        var channelsFromPortalSlots = foundOnPortal.parallelStream()
            .collect(Collectors.toConcurrentMap(Function.identity(), slotUri ->
                channelManagerClient
                    .create(GrpcConverter.createChannelRequest(executionId,
                        createChannelSpec("portal_channel_" + slotUri)))
                    .getChannelId()));

        LMS3.S3Locator storageData;
        String portalAddress;
        try {
            storageData = withRetries(LOG, () -> workflowDao.getS3CredentialsFor(executionId));
            portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddressFor(executionId));
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        var portalChannel = ChannelBuilder.forAddress(portalAddress).usePlaintext().build();
        LzyPortalGrpc.LzyPortalBlockingStub portalClient = LzyPortalGrpc.newBlockingStub(portalChannel);

        String bucket = storageData.getBucket();
        String endpoint = storageData.getAmazon().getEndpoint();
        String accessToken = storageData.getAmazon().getAccessToken();
        String secretToken = storageData.getAmazon().getSecretToken();

        var inputPortalSlotsDesc = new ArrayList<PortalSlotDesc>();
        for (var slotUriAndChannel : channelsFromOutputSlots.entrySet()) {
            var slotUri = slotUriAndChannel.getKey();
            var portalSlotName = "/portal_slot_" + slotUri;
            var snapshot = PortalSlotDesc.Snapshot.newBuilder()
                .setS3(LMS3.S3Locator.newBuilder()
                    .setKey(slotUri)
                    .setBucket(bucket)
                    .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                        .setAccessToken(accessToken)
                        .setSecretToken(secretToken)
                        .setEndpoint(endpoint)
                        .build()))
                .build();

            inputPortalSlotsDesc.add(PortalSlotDesc.newBuilder()
                .setSnapshot(snapshot)
                .setSlot(LMS.Slot.newBuilder()
                    .setName(portalSlotName)
                    .setMedia(LMS.Slot.Media.FILE)
                    .setDirection(LMS.Slot.Direction.INPUT)
                    .setContentType(LMD.DataScheme.newBuilder()
                        .setSchemeContent("text")
                        .setDataFormat("plain")
                        .build())
                    .build())
                .setChannelId(slotUriAndChannel.getValue())
                .build());
        }

        var outputPortalSlotsDesc = new ArrayList<PortalSlotDesc>();
        for (var slotUriAndChannel : channelsFromPortalSlots.entrySet()) {
            var slotUri = slotUriAndChannel.getKey();
            var portalSlotName = "/portal_slot_" + slotUri;
            var snapshot = PortalSlotDesc.Snapshot.newBuilder()
                .setS3(LMS3.S3Locator.newBuilder()
                    .setKey(slotUri)
                    .setBucket(bucket)
                    .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                        .setAccessToken(accessToken)
                        .setSecretToken(secretToken)
                        .setEndpoint(endpoint)
                        .build()))
                .build();

            outputPortalSlotsDesc.add(PortalSlotDesc.newBuilder()
                .setSnapshot(snapshot)
                .setSlot(LMS.Slot.newBuilder()
                    .setName(portalSlotName)
                    .setMedia(LMS.Slot.Media.FILE)
                    .setDirection(LMS.Slot.Direction.OUTPUT)
                    .setContentType(LMD.DataScheme.newBuilder()
                        .setSchemeContent("text")
                        .setDataFormat("plain")
                        .build())
                    .build())
                .setChannelId(slotUriAndChannel.getValue())
                .build());
        }

        var response = portalClient.openSlots(OpenSlotsRequest.newBuilder()
            .addAllSlots(inputPortalSlotsDesc)
            .addAllSlots(outputPortalSlotsDesc)
            .build());

        portalChannel.shutdown();

        if (!response.getSuccess()) {
            LOG.error("Cannot open portal slots for tasks slots { executionId: {}, workflowName: {} }",
                executionId, workflowName);
            throw new RuntimeException(response.getDescription());
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                executionDao.indicateSlotsUriStoredOnPortal(executionId, slotsUriAsOutput));
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        var slotsUri2channels = new HashMap<String, String>() {
            {
                putAll(channelsFromOutputSlots);
                putAll(channelsFromPortalSlots);
            }
        };

        return Stream.concat(inputSlotDescriptions.parallelStream(), outputSlotDescriptions.parallelStream())
            .parallel()
            .collect(Collectors.toConcurrentMap(SlotDescription::getPath,
                slot -> slotsUri2channels.get(slot.getStorageUri())));
    }

    private String[] createPortalStdChannels(String executionId) {
        LOG.info("Creating portal stdout channel with name '{}'", startupPortalConfig.getStdoutChannelName());
        // create portal stdout channel that receives portal output
        var stdoutChannelId = channelManagerClient.create(GrpcConverter.createChannelRequest(executionId,
            createChannelSpec(startupPortalConfig.getStdoutChannelName()))).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", startupPortalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        var stderrChannelId = channelManagerClient.create(GrpcConverter.createChannelRequest(executionId,
            createChannelSpec(startupPortalConfig.getStderrChannelName()))).getChannelId();

        return new String[] {stdoutChannelId, stderrChannelId};
    }

    private static LCM.ChannelSpec createChannelSpec(String channelName) {
        return LCM.ChannelSpec.newBuilder()
            .setChannelName(channelName)
            .setContentType(LMD.DataScheme.newBuilder()
                .setSchemeContent("text")
                .setDataFormat("plain")
                .build())
            .setDirect(LCM.DirectChannelType.getDefaultInstance())
            .build();
    }

    public String createSession(String userId) {
        LOG.info("Creating session for user with id '{}'", userId);

        VmAllocatorApi.CreateSessionResponse session = allocatorClient.createSession(
            VmAllocatorApi.CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder().setIdleTimeout(Durations.ZERO))
                .build());
        return session.getSessionId();
    }

    public OperationService.Operation startAllocation(String workflowName, String sessionId, String executionId,
                                                      String stdoutChannelId, String stderrChannelId)
    {
        var portalId = "portal_" + executionId + UUID.randomUUID();

        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            var publicKey = Files.readString(workerKeys.publicKeyPath());
            privateKey = Files.readString(workerKeys.privateKeyPath());

            final var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.SERVANT,
                new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));

            abClient.setAccessBindings(new Workflow(workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal", e);
            throw new RuntimeException(e);
        }

        var args = List.of(
            "-portal.portal-id=" + portalId,
            "-portal.portal-api-port=" + startupPortalConfig.getPortalApiPort(),
            "-portal.fs-api-port=" + startupPortalConfig.getFsApiPort(),
            "-portal.fs-root=" + startupPortalConfig.getFsRoot(),
            "-portal.stdout-channel-id=" + stdoutChannelId,
            "-portal.stderr-channel-id=" + stderrChannelId,
            "-portal.channel-manager-address=" + channelManagerAddress);

        var ports = Map.of(
            startupPortalConfig.getFsApiPort(), startupPortalConfig.getFsApiPort(),
            startupPortalConfig.getPortalApiPort(), startupPortalConfig.getPortalApiPort()
        );

        return allocatorClient.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder().setSessionId(sessionId).setPoolLabel("portals")
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                    .setName("portal")
                    // TODO: ssokolvyak -- fill the image in production
                    //.setImage(portalConfig.getPortalImage())
                    .addAllArgs(args)
                    .putEnv(ENV_PORTAL_PKEY, privateKey)
                    .putAllPortBindings(ports)
                    .build())
                .build());
    }

    @Nullable
    public VmAllocatorApi.AllocateResponse waitAllocation(Instant deadline, String operationId) {
        // TODO: ssokolvyak -- replace on streaming request
        OperationService.Operation allocateOperation;

        while (Instant.now().isBefore(deadline)) {
            allocateOperation = operationServiceClient.get(OperationService.GetOperationRequest.newBuilder()
                .setOperationId(operationId).build());
            if (allocateOperation.getDone()) {
                try {
                    return allocateOperation.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Cannot deserialize allocate response from operation with id: " + operationId);
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
        }
        return null;
    }

    public LMS3.S3Locator createTempStorageBucket(String userId) {
        var bucket = "tmp-bucket-" + userId;
        LOG.info("Creating new temp storage bucket '{}' for user '{}'", bucket, userId);

        LSS.CreateS3BucketResponse response = storageServiceClient.createS3Bucket(
            LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucket)
                .build());

        // there something else except AMAZON or AZURE may be returned here?
        return switch (response.getCredentialsCase()) {
            case AMAZON -> LMS3.S3Locator.newBuilder().setAmazon(response.getAmazon()).setBucket(bucket).build();
            case AZURE -> LMS3.S3Locator.newBuilder().setAzure(response.getAzure()).setBucket(bucket).build();
            default -> {
                LOG.error("Unsupported bucket storage type {}", response.getCredentialsCase());
                safeDeleteTempStorageBucket(bucket);
                yield null;
            }
        };
    }

    private void safeDeleteTempStorageBucket(String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storageServiceClient.deleteS3Bucket(
                LSS.DeleteS3BucketRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }

    public enum PortalStatus {
        CREATING_STD_CHANNELS, CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
