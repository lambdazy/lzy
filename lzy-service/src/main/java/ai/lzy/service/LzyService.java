package ai.lzy.service;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.vmpool.VmPoolClient;
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
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.DataFlowGraph;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.*;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.*;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.OpenSlotsRequest;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
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
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.PredicateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.grpc.ProtoConverter.createChannelRequest;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.v1.workflow.LWFS.ExecuteGraphRequest;
import static ai.lzy.v1.workflow.LWFS.ExecuteGraphResponse;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final Duration allocationTimeout;

    private final LzyServiceConfig.StartupPortalConfig startupPortalConfig;
    private final String channelManagerAddress;
    private final String iamAddress;
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
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;

    private final ManagedChannel iamChannel;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;

    private final ManagedChannel graphExecutorChannel;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    private final Map<String, ManagedChannel> portalChannelForExecution = new ConcurrentHashMap<>();

    public LzyService(LzyServiceConfig config, LzyServiceStorage storage,
                      WorkflowDao workflowDao, ExecutionDao executionDao)
    {
        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        startupPortalConfig = config.getPortal();
        allocationTimeout = config.getWaitAllocationTimeout();
        channelManagerAddress = config.getChannelManagerAddress();

        iamAddress = config.getIam().getAddress();
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
            .enableRetry(LzyChannelManagerPrivateGrpc.SERVICE_NAME)
            .build();
        channelManagerClient = LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));

        iamChannel = ChannelBuilder
            .forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();

        subjectClient = new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
        abClient = new AccessBindingServiceGrpcClient(iamChannel, config.getIam()::createCredentials);

        graphExecutorChannel = ChannelBuilder.forAddress(config.getGraphExecutorAddress()).build();
        graphExecutorClient = GraphExecutorGrpc.newBlockingStub(graphExecutorChannel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                internalUserCredentials::token));
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
        channelManagerChannel.shutdown();
        graphExecutorChannel.shutdown();
        iamChannel.shutdown();
    }

    @Override
    public void createWorkflow(LWFS.CreateWorkflowRequest request,
                               StreamObserver<LWFS.CreateWorkflowResponse> response)
    {
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
                workflowDao.create(executionId, userId, workflowName, storageType, storageData));
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

            var result = LWFS.CreateWorkflowResponse.newBuilder().setExecutionId(executionId);
            if (internalSnapshotStorage) {
                result.setInternalSnapshotStorage(storageData);
            }
            response.onNext(result.build());
            response.onCompleted();
        }
    }

    @Override
    public void attachWorkflow(LWFS.AttachWorkflowRequest request,
                               StreamObserver<LWFS.AttachWorkflowResponse> response)
    {
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

                response.onNext(LWFS.AttachWorkflowResponse.getDefaultInstance());
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
    public void finishWorkflow(LWFS.FinishWorkflowRequest request,
                               StreamObserver<LWFS.FinishWorkflowResponse> response)
    {
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

        response.onNext(LWFS.FinishWorkflowResponse.getDefaultInstance());
        response.onCompleted();

        // TODO: add TTL instead of implicit delete
        // safeDeleteTempStorageBucket(bucket[0]);
    }

    private boolean startPortal(String workflowName, String executionId, String userId,
                                StreamObserver<LWFS.CreateWorkflowResponse> response)
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

        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot start portal for: { userId: {}, workflowName: {}, executionId: {} } , error: {}",
                userId, workflowName, executionId, causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot start portal: " + causeStatus.getDescription())
                .asRuntimeException());
            return false;
        } catch (Exception e) {
            response.onError(Status.INTERNAL.withDescription("Cannot start portal: " + e.getMessage())
                .asRuntimeException());
            return false;
        }
        return true;
    }

    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> response) {
        var executionId = request.getExecutionId();

        String workflowName;
        try {
            workflowName = withRetries(LOG, () -> workflowDao.getWorkflowNameBy(executionId));
        } catch (NotFoundException e) {
            LOG.error("Cannot obtain workflow name for execution { executionId: {} }: " + e.getMessage(), executionId);
            response.onError(Status.NOT_FOUND.withDescription("Cannot obtain workflow name: " + e.getMessage())
                .asRuntimeException());
            return;
        } catch (Exception e) {
            LOG.error("Cannot obtain workflow name for execution { executionId: {} }: " + e.getMessage(), executionId);
            response.onError(Status.INTERNAL.withDescription("Cannot obtain workflow name: " + e.getMessage())
                .asRuntimeException());
            return;
        }

        LWF.Graph graph = request.getGraph();
        Collection<LWF.Operation> operations = graph.getOperationsList();
        var slotsUriAsOutput = operations.stream()
            .flatMap(op -> op.getOutputSlotsList().stream().map(LWF.Operation.SlotDescription::getStorageUri))
            .collect(Collectors.toSet());
        var duplicate = findFirstDuplicate(slotsUriAsOutput);

        if (duplicate != null) {
            LOG.error("Duplicated output slot URI { executionId: {}, slotUri: {}, workflowName: {}} ",
                executionId, duplicate, workflowName);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Duplicated slot uri: " + duplicate)
                .asRuntimeException());
            return;
        }

        Set<String> knownSlots;
        try {
            knownSlots = withRetries(LOG, () -> executionDao.retainExistingSlots(slotsUriAsOutput));
        } catch (Exception e) {
            LOG.error("Cannot obtain existing slots URIs while starting graph for execution " +
                "{ executionId: {}, workflowName: {} }", executionId, workflowName);
            response.onError(Status.INTERNAL
                .withDescription("Cannot check that slots URIs are not already used as output. " +
                    "Cannot obtain existing slots uri: " + e.getMessage()).asRuntimeException());
            return;
        }

        if (!knownSlots.isEmpty()) {
            LOG.error("Output slots URIs { slotsUri: {} } already used in other execution",
                JsonUtils.printAsArray(knownSlots));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Output slots URIs already used " +
                "on other execution: " + JsonUtils.printAsArray(knownSlots)).asRuntimeException());
            return;
        }

        var dataflowGraph = new DataFlowGraph(operations);

        if (dataflowGraph.hasCycle()) {
            LOG.error("Try to execute cyclic graph { executionId: {}, workflowName: {}, cycle: {} }",
                executionId, workflowName, dataflowGraph.printCycle());
            response.onError(Status.INVALID_ARGUMENT.withDescription("Operations graph has cycle: " +
                dataflowGraph.printCycle()).asRuntimeException());
            return;
        }

        Set<String> unknownSlots;
        Set<String> fromPortal = dataflowGraph.getDanglingInputSlots().keySet();
        try {
            unknownSlots = withRetries(LOG, () -> executionDao.retainNonExistingSlots(executionId, fromPortal));
        } catch (Exception e) {
            LOG.error("Cannot obtain non-existing slots URIs associated with execution " +
                "{ executionId: {}, workflowName: {} }", executionId, workflowName);
            response.onError(Status.INTERNAL
                .withDescription("Cannot check that slots URIs are stored on portal. " +
                    "Cannot obtain non-existing slots URIs: " + e.getMessage()).asRuntimeException());
            return;
        }

        if (!unknownSlots.isEmpty()) {
            LOG.error("Slots URIs { slotUris: {} } are presented neither in output slots URIs " +
                "nor stored as already associated with portal", JsonUtils.printAsArray(unknownSlots));
            response.onError(Status.NOT_FOUND.withDescription("Slots URIs not found: " +
                JsonUtils.printAsArray(unknownSlots)).asRuntimeException());
            return;
        }

        var requiredPoolLabels = graph.getOperationsList().stream().map(LWF.Operation::getPoolSpecName).toList();
        Set<String> suitableZones;

        try {
            suitableZones = VmPoolClient.findZonesContainAllRequiredPools(vmPoolClient, requiredPoolLabels);
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot obtain vm pools for { poolLabels: {} } , error: {}",
                JsonUtils.printAsArray(requiredPoolLabels), causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot obtain vm pools: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        var zoneName = graph.getZone().isBlank() ? suitableZones.stream().findAny().orElse(null) : graph.getZone();

        if (zoneName == null) {
            LOG.error("Cannot find zone which has all required pools: { poolLabels: {} }",
                JsonUtils.printAsArray(requiredPoolLabels));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find zone which has all required pools")
                .asRuntimeException());
            return;
        }

        if (!suitableZones.contains(zoneName)) {
            LOG.error("Passed zone does not contain all required pools: { zoneName: {}, poolLabels: {} }",
                zoneName, JsonUtils.printAsArray(requiredPoolLabels));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Passed zone does not contains all required pools")
                .asRuntimeException());
            return;
        }

        ManagedChannel portalChannel;
        try {
            portalChannel = portalChannelForExecution.computeIfAbsent(executionId, exId -> {
                String portalAddress;
                try {
                    portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddressFor(exId));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return ChannelBuilder.forAddress(portalAddress)
                    .enableRetry(LzyPortalGrpc.SERVICE_NAME)
                    .usePlaintext()
                    .build();
            });
        } catch (RuntimeException e) {
            var cause = Objects.nonNull(e.getCause()) ? e.getCause() : e;
            LOG.error("Cannot obtain information about portal for execution: { executionId: {} } " +
                cause.getMessage(), executionId);
            response.onError(Status.INTERNAL.withDescription("Cannot obtain information about portal: " +
                cause.getMessage()).asRuntimeException());
            return;
        }

        LzyPortalGrpc.LzyPortalBlockingStub portalClient = LzyPortalGrpc.newBlockingStub(portalChannel);

        // slot name to channel id
        Map<String, String> slots2channels;
        try {
            slots2channels = assignSlotsToChannels(executionId, workflowName,
                dataflowGraph.getDataFlow(), portalClient);
        } catch (Exception e) {
            LOG.error("Cannot assign slots to channels for execution: { executionId: {}, workflowName: {} } " +
                e.getMessage(), executionId, workflowName);
            response.onError(Status.INTERNAL.withDescription("Cannot assign slots to channels: " + e.getMessage())
                .asRuntimeException());
            return;
        }

        var slot2description = graph.getDataDescriptionsList().stream()
            .collect(Collectors.toMap(LWF.DataDescription::getStorageUri, Function.identity()));

        List<GraphExecutor.TaskDesc> tasks;
        try {
            tasks = buildTasksWithZone(zoneName, executionId, graph.getOperationsList(),
                slots2channels, slot2description, portalClient);
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot build tasks for execution: { executionId: {}, workflowName: {} }, error: {} ",
                executionId, workflowName, e.getMessage());
            response.onError(causeStatus.withDescription("Cannot build tasks: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        } catch (Exception e) {
            LOG.error("Cannot build tasks for execution: { executionId: {}, workflowName: {} }, error: {} " +
                e.getMessage(), executionId, workflowName);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Error while building tasks: " + e.getMessage())
                .asRuntimeException());
            return;
        }

        List<GraphExecutor.ChannelDesc> channels = buildChannelDescriptions(slots2channels.values());

        GraphExecutorApi.GraphExecuteResponse executeResponse;
        try {
            executeResponse = graphExecutorClient.execute(GraphExecutorApi.GraphExecuteRequest.newBuilder()
                .setWorkflowId(executionId)
                .setWorkflowName(workflowName)
                .setParentGraphId(graph.getParentGraphId())
                .addAllTasks(tasks)
                .addAllChannels(channels)
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot execute graph in: { workflowName: {}, executionId: {} }, error: {}", workflowName,
                executionId, causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot execute graph: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        response.onNext(ExecuteGraphResponse.newBuilder().setGraphId(executeResponse.getStatus().getGraphId()).build());
        response.onCompleted();
    }

    @Override
    public void graphStatus(LWFS.GraphStatusRequest request, StreamObserver<LWFS.GraphStatusResponse> response) {
        var executionId = request.getExecutionId();
        var graphId = request.getGraphId();

        GraphExecutorApi.GraphStatusResponse graphStatus;
        try {
            graphStatus = graphExecutorClient.status(GraphExecutorApi.GraphStatusRequest.newBuilder()
                .setWorkflowId(executionId)
                .setGraphId(graphId)
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot obtain graph status: { executionId: {}, graphId: {} }, error: {}",
                executionId, graphId, causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot obtain graph status: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        if (!graphStatus.hasStatus()) {
            LOG.error("Empty graph status for graph: { executionId: {}, graphId: {} }", executionId, graphId);
            response.onError(Status.INTERNAL.withDescription("Empty graph status for graph").asRuntimeException());
            return;
        }

        var graphStatusResponse = LWFS.GraphStatusResponse.newBuilder();

        switch (graphStatus.getStatus().getStatusCase()) {
            case WAITING -> graphStatusResponse.setWaiting(LWFS.GraphStatusResponse.Waiting.getDefaultInstance());
            case EXECUTING -> {
                var allTaskStatuses = graphStatus.getStatus().getExecuting().getExecutingTasksList();

                var executingTaskIds = new ArrayList<String>();
                var completedTaskIds = new ArrayList<String>();
                var waitingTaskIds = new ArrayList<String>();

                allTaskStatuses.forEach(status -> {
                    var taskId = status.getTaskDescriptionId();

                    if (!status.hasProgress()) {
                        LOG.error("Empty task status: { executionId: {}, graphId: {}, taskId: {} }", executionId,
                            graphId, taskId);
                        response.onError(Status.INTERNAL.withDescription("Empty status of task with ID: " + taskId)
                            .asRuntimeException());
                        return;
                    }

                    switch (status.getProgress().getStatusCase()) {
                        case QUEUE -> waitingTaskIds.add(taskId);
                        case EXECUTING -> executingTaskIds.add(taskId);
                        case SUCCESS, ERROR -> completedTaskIds.add(taskId);
                    }
                });

                graphStatusResponse.setExecuting(LWFS.GraphStatusResponse.Executing.newBuilder()
                    .setMessage("Graph status")
                    .addAllOperationsExecuting(executingTaskIds)
                    .addAllOperationsCompleted(completedTaskIds)
                    .addAllOperationsWaiting(waitingTaskIds));
            }
            case COMPLETED -> graphStatusResponse.setCompleted(LWFS.GraphStatusResponse.Completed.getDefaultInstance());
            case FAILED -> graphStatusResponse.setFailed(LWFS.GraphStatusResponse.Failed.newBuilder()
                .setDescription(graphStatus.getStatus().getFailed().getDescription()));
        }

        response.onNext(graphStatusResponse.build());
        response.onCompleted();
    }

    @Override
    public void stopGraph(LWFS.StopGraphRequest request, StreamObserver<LWFS.StopGraphResponse> response) {
        var executionId = request.getExecutionId();
        var graphId = request.getGraphId();

        try {
            //noinspection ResultOfMethodCallIgnored
            graphExecutorClient.stop(GraphExecutorApi.GraphStopRequest.newBuilder()
                .setWorkflowId(executionId)
                .setGraphId(graphId)
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot stop graph: { executionId: {}, graphId: {} }, error: {}",
                executionId, graphId, causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot stop graph: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        response.onNext(LWFS.StopGraphResponse.getDefaultInstance());
        response.onCompleted();
    }

    private List<GraphExecutor.ChannelDesc> buildChannelDescriptions(Collection<String> channelIds) {
        return channelIds.stream()
            .map(id -> GraphExecutor.ChannelDesc
                .newBuilder()
                .setId(id)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build())
            .toList();
    }

    private GraphExecutor.TaskDesc buildTaskWithZone(String executionId, String taskId, LWF.Operation operation,
                                                     String zoneName, String stdoutChannelId, String stderrChannelId,
                                                     Map<String, String> slot2Channel,
                                                     Map<String, LWF.DataDescription> slot2description,
                                                     LzyPortalGrpc.LzyPortalBlockingStub portalClient)
    {
        var env = LME.EnvSpec.newBuilder();
        if (!operation.getDockerImage().isBlank()) {
            env.setBaseEnv(LME.BaseEnv.newBuilder().setName(operation.getDockerImage()).build());
        }
        if (operation.hasPython()) {
            env.setAuxEnv(LME.AuxEnv.newBuilder().setPyenv(
                    LME.PythonEnv.newBuilder()
                        .setYaml(operation.getPython().getYaml())
                        .addAllLocalModules(
                            operation.getPython().getLocalModulesList().parallelStream()
                                .map(module -> LME.LocalModule.newBuilder()
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
                LOG.error("Cannot find data description for input slot: " + slotDescription.getPath());
                return null;
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
                LOG.error("Cannot find data description for output slot: " + slotDescription.getPath());
                return null;
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

        LzyPortalApi.OpenSlotsResponse response = portalClient.openSlots(OpenSlotsRequest.newBuilder()
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

        if (!response.getSuccess()) {
            LOG.error("Cannot open portal slots for { executionId: {} }: " + response.getDescription(), executionId);
            throw new RuntimeException("Cannot open portal slots: " + response.getDescription());
        }

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

        return GraphExecutor.TaskDesc.newBuilder()
            .setId(taskId)
            .setOperation(taskOperation)
            .addAllSlotAssignments(slotToChannelAssignments)
            .build();
    }

    private List<GraphExecutor.TaskDesc> buildTasksWithZone(String executionId, String zoneName,
                                                            Collection<LWF.Operation> operations,
                                                            Map<String, String> slot2Channel,
                                                            Map<String, LWF.DataDescription> slot2description,
                                                            LzyPortalGrpc.LzyPortalBlockingStub portalClient)
    {
        var tasks = ListUtils.predicatedList(new ArrayList<GraphExecutor.TaskDesc>(operations.size()),
            PredicateUtils.notNullPredicate());

        for (var operation : operations) {
            // TODO: ssokolvyak -- must not be generated here but passed in executeGraph request
            var taskId = UUID.randomUUID().toString();

            var stdoutChannelName = taskId + ":stdout";
            var stderrChannelName = taskId + ":stderr";
            var stdoutChannelId = createChannel(stdoutChannelName);
            var stderrChannelId = createChannel(stderrChannelName);

            tasks.add(buildTaskWithZone(executionId, taskId, operation, zoneName,
                stdoutChannelId, stderrChannelId, slot2Channel, slot2description, portalClient));
        }

        return tasks;
    }

    private String createChannel(String name) {
        return channelManagerClient.create(LCMPS.ChannelCreateRequest.newBuilder()
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
                                                      Collection<DataFlowGraph.Data> dataFlow,
                                                      LzyPortalGrpc.LzyPortalBlockingStub portalClient)
    {
        var slotName2channelId = new HashMap<String, String>();
        var partitionBySupplier = dataFlow.stream().collect(Collectors.partitioningBy(data -> data.supplier() != null));
        var fromOutput = partitionBySupplier.get(true);
        var fromPortal = partitionBySupplier.get(false);

        LMS3.S3Locator storageData;
        try {
            storageData = withRetries(LOG, () -> workflowDao.getS3CredentialsFor(executionId));
        } catch (Exception e) {
            LOG.error("Cannot obtain information about snapshots storage for execution: { executionId: {} } " +
                e.getMessage(), executionId);
            throw new RuntimeException("Cannot obtain information about snapshots storage: " + e.getMessage());
        }

        String bucket = storageData.getBucket();
        String endpoint = storageData.getAmazon().getEndpoint();
        String accessToken = storageData.getAmazon().getAccessToken();
        String secretToken = storageData.getAmazon().getSecretToken();

        var portalSlotToOpen = new ArrayList<PortalSlotDesc>();

        for (var data : fromOutput) {
            // from output slot
            var slotUri = data.slotUri();
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
            var portalInputSlotName = "/portal_slot_" + slotUri;

            var channelName = "channel_" + slotUri;
            var channelId = channelManagerClient
                .create(createChannelRequest(executionId, createChannelSpec(channelName)))
                .getChannelId();

            portalSlotToOpen.add(PortalSlotDesc.newBuilder()
                .setSnapshot(snapshot)
                .setSlot(LMS.Slot.newBuilder()
                    .setName(portalInputSlotName)
                    .setMedia(LMS.Slot.Media.FILE)
                    .setDirection(LMS.Slot.Direction.INPUT)
                    .setContentType(LMD.DataScheme.newBuilder()
                        .setSchemeContent("text")
                        .setDataFormat("plain")
                        .build())
                    .build())
                .setChannelId(channelId)
                .build());


            slotName2channelId.put(data.supplier(), channelId);
            // slotName2channelId.put(portalInputSlotName, channelId); // info about portal slots should not be passed
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }
        }

        Map<String, String> outputSlot2channel;

        synchronized (this) {
            var slotsUri = dataFlow.stream().map(DataFlowGraph.Data::slotUri).collect(Collectors.toSet());

            try {
                outputSlot2channel = withRetries(LOG, () -> executionDao.findChannelsForOutputSlots(slotsUri));
            } catch (Exception e) {
                LOG.error("Cannot obtain information about channels for slots. Execution: { executionId: {} } " +
                    e.getMessage(), executionId);
                throw new RuntimeException("Cannot obtain information about channels for slots: " + e.getMessage());
            }

            var withoutChannels = fromPortal.stream()
                .filter(data -> !outputSlot2channel.containsKey(data.slotUri())).toList();

            var newChannels = new HashMap<String, String>();

            for (var data : withoutChannels) {
                var slotUri = data.slotUri();
                var channelId = channelManagerClient
                    .create(createChannelRequest(executionId,
                        createChannelSpec("portal_channel_" + slotUri)))
                    .getChannelId();

                newChannels.put(slotUri, channelId);

                var portalOutputSlotName = "/portal_slot_" + slotUri;
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

                portalSlotToOpen.add(PortalSlotDesc.newBuilder()
                    .setSnapshot(snapshot)
                    .setSlot(LMS.Slot.newBuilder()
                        .setName(portalOutputSlotName)
                        .setMedia(LMS.Slot.Media.FILE)
                        .setDirection(LMS.Slot.Direction.OUTPUT)
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setSchemeContent("text")
                            .setDataFormat("plain")
                            .build())
                        .build())
                    .setChannelId(channelId)
                    .build());
            }

            try {
                withRetries(defaultRetryPolicy(), LOG, () -> executionDao.saveChannels(newChannels));
            } catch (Exception e) {
                LOG.error("Cannot save information about channels for execution: { executionId: {} } " +
                    e.getMessage(), executionId);
                throw new RuntimeException("Cannot save information about channels: " + e.getMessage());
            }

            outputSlot2channel.putAll(newChannels);
        }

        for (var data : fromPortal) {
            // from portal slot
            var slotUri = data.slotUri();
            var channelId = outputSlot2channel.get(slotUri);

            // slotName2channelId.put(portalOutputSlotName, channelId); // info about portal slots should not be passed
            if (data.consumers() != null) {
                for (var consumer : data.consumers()) {
                    slotName2channelId.put(consumer, channelId);
                }
            }
        }

        var response = portalClient.openSlots(OpenSlotsRequest.newBuilder().addAllSlots(portalSlotToOpen).build());

        if (!response.getSuccess()) {
            LOG.error("Cannot open portal slots for tasks slots { executionId: {}, workflowName: {} }",
                executionId, workflowName);
            throw new RuntimeException(response.getDescription());
        }

        var slotsUriAsOutput = fromOutput.stream().map(DataFlowGraph.Data::slotUri).collect(Collectors.toSet());
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> executionDao.saveSlots(executionId, slotsUriAsOutput));
        } catch (Exception e) {
            LOG.error("Cannot save information to internal storage: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        return slotName2channelId;
    }

    private String[] createPortalStdChannels(String executionId) {
        LOG.info("Creating portal stdout channel with name '{}'", startupPortalConfig.getStdoutChannelName());
        // create portal stdout channel that receives portal output
        var stdoutChannelId = channelManagerClient.create(createChannelRequest(executionId,
            createChannelSpec(startupPortalConfig.getStdoutChannelName()))).getChannelId();

        LOG.info("Creating portal stderr channel with name '{}'", startupPortalConfig.getStderrChannelName());
        // create portal stderr channel that receives portal error output
        var stderrChannelId = channelManagerClient.create(createChannelRequest(executionId,
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
            "-portal.channel-manager-address=" + channelManagerAddress,
            "-portal.iam-address=" + iamAddress);

        var ports = Map.of(
            startupPortalConfig.getFsApiPort(), startupPortalConfig.getFsApiPort(),
            startupPortalConfig.getPortalApiPort(), startupPortalConfig.getPortalApiPort()
        );

        return allocatorClient.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder().setSessionId(sessionId).setPoolLabel("portals")
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                    .setName("portal")
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
