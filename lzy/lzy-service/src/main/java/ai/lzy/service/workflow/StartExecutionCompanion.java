package ai.lzy.service.workflow;

import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.model.Constants;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.service.kafka.KafkaClient;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.iam.grpc.context.AuthenticationContext.currentSubject;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.workflow.WorkflowService.LOG;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class StartExecutionCompanion {
    private final CreateExecutionState state;
    private final WorkflowService owner;
    private final LzyServiceConfig.StartupPortalConfig cfg;

    StartExecutionCompanion(CreateExecutionState initial,
                            WorkflowService owner, LzyServiceConfig.StartupPortalConfig cfg)
    {
        this.state = initial;
        this.owner = owner;
        this.cfg = cfg;
    }

    static StartExecutionCompanion of(LWFS.StartWorkflowRequest request, WorkflowService owner,
                                      LzyServiceConfig.StartupPortalConfig cfg)
    {
        var initState =
            new CreateExecutionState(currentSubject().id(), request.getWorkflowName(), request.getStorageName(),
                request.getSnapshotStorage());
        return new StartExecutionCompanion(initState, owner, cfg);
    }

    public boolean isInvalid() {
        return state.isInvalid();
    }

    @Nullable
    public Status getErrorStatus() {
        return state.getErrorStatus();
    }

    public String getExecutionId() {
        return state.getExecutionId();
    }

    public String getOwner() {
        return state.getUserId();
    }

    public CreateExecutionState getState() {
        return state;
    }

    /**
     * Returns previous active execution id.
     */
    @Nullable
    public String createExecutionInDao() {
        String prevExecutionId = null;

        try (var tx = TransactionHandle.create(owner.storage)) {
            withRetries(LOG,
                () -> owner.executionDao.create(state.getUserId(), state.getExecutionId(), state.getStorageName(),
                    state.getStorageConfig(), tx));
            prevExecutionId = withRetries(LOG,
                () -> owner.workflowDao.upsert(state.getUserId(), state.getWorkflowName(), state.getExecutionId(), tx));

            tx.commit();
        } catch (Exception e) {
            LOG.error("Error while creating execution state in dao", e);
            state.fail(Status.INTERNAL, "Cannot create execution: " + e.getMessage());
        }

        return prevExecutionId;
    }

    public void checkStorage() {
        var userStorage = state.getStorageConfig();
        if (userStorage.getCredentialsCase() == LMST.StorageConfig.CredentialsCase.CREDENTIALS_NOT_SET) {
            state.fail(Status.INVALID_ARGUMENT, "Credentials are not set");
        }
    }

    public void startPortal(String dockerImage, int portalPort, int slotsApiPort,
                            String stdoutChannelName, String stderrChannelName,
                            String channelManagerAddress, String iamAddress, String whiteboardAddress,
                            Duration allocationTimeout, Duration allocateVmCacheTimeout, boolean createStdChannels)
    {
        LOG.info("Attempt to start portal for workflow execution: { wfName: {}, execId: {} }",
            state.getWorkflowName(), state.getExecutionId());

        try {
            InjectedFailures.fail9();

            if (createStdChannels) {
                createPortalStdChannels(stdoutChannelName, stderrChannelName);

                withRetries(LOG, () -> owner.executionDao.updateStdChannelIds(state.getExecutionId(),
                    state.getStdoutChannelId(), state.getStderrChannelId(), null));
            }

            createAllocatorSession(allocateVmCacheTimeout);

            state.setPortalId("portal_" + state.getExecutionId() + UUID.randomUUID());

            withRetries(LOG, () -> owner.executionDao.updatePortalVmAllocateSession(state.getExecutionId(),
                state.getSessionId(), state.getPortalId(), null));

            InjectedFailures.fail10();

            var allocateVmOp = startAllocation(dockerImage, channelManagerAddress, iamAddress,
                whiteboardAddress, portalPort, slotsApiPort);
            var opId = allocateVmOp.getId();

            VmAllocatorApi.AllocateMetadata allocateMetadata;
            try {
                allocateMetadata = allocateVmOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
            } catch (InvalidProtocolBufferException e) {
                state.fail(Status.INTERNAL,
                    "Invalid allocate operation metadata: VM id missed. Operation id: " + opId);
                return;
            }
            var vmId = allocateMetadata.getVmId();

            withRetries(LOG, () ->
                owner.executionDao.updateAllocateOperationData(state.getExecutionId(), opId, vmId, null));

            InjectedFailures.fail11();

            allocateVmOp = awaitOperationDone(owner.allocOpService, opId, allocationTimeout);

            if (!allocateVmOp.getDone()) {
                state.fail(Status.DEADLINE_EXCEEDED,
                    "Cannot wait allocate operation response. Operation id: " + opId);
                return;
            }

            if (allocateVmOp.hasError()) {
                var status = allocateVmOp.getError();
                state.fail(Status.fromCodeValue(status.getCode()), "Cannot process allocate vm operation: " +
                    "{ operationId: %s }, error: %s".formatted(allocateVmOp.getId(), status.getMessage()));
                return;
            }

            var allocateResponse = allocateVmOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);

            withRetries(LOG, () -> owner.executionDao.updatePortalVmAddress(
                state.getExecutionId(),
                allocateResponse.getMetadataOrDefault(Constants.PORTAL_ADDRESS_KEY, null),
                allocateResponse.getMetadataOrDefault(Constants.FS_ADDRESS_KEY, null),
                /* transaction */ null
            ));

            InjectedFailures.fail12();
        } catch (InjectedFailures.TerminateException e) {
            LOG.error("Got InjectedFailure exception: " + e.getMessage());
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot start portal", e);
            state.fail(e.getStatus(), "Cannot start portal");
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot deserialize allocate response from operation: " + e.getMessage());
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot start portal: " + e.getMessage());
        }
    }

    // create channels thar receive portal stdout/stderr
    private void createPortalStdChannels(String stdoutName, String stderrName) {
        LOG.info("Creating portal stdout channel: { channelName: {} }", stdoutName);

        var stdoutChannelId = owner.channelManagerClient.create(makeCreateChannelCommand(state.getUserId(),
            state.getWorkflowName(), state.getExecutionId(), stdoutName)).getChannelId();
        state.setStdoutChannelId(stdoutChannelId);

        LOG.info("Creating portal stderr channel: { channelName: {} }", stderrName);

        var stderrChannelId = owner.channelManagerClient.create(makeCreateChannelCommand(state.getUserId(),
            state.getWorkflowName(), state.getExecutionId(), stderrName)).getChannelId();
        state.setStderrChannelId(stderrChannelId);
    }

    public void createAllocatorSession(Duration allocatorVmCacheTimeout) {
        LOG.info("Creating session for: { userId: {}, workflowName: {}, executionId: {} }", state.getUserId(),
            state.getWorkflowName(), state.getExecutionId());

        var op = withIdempotencyKey(owner.allocatorClient, state.getExecutionId())
            .createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(state.getUserId())
                    .setDescription(state.getExecutionId())
                    .setCachePolicy(
                        VmAllocatorApi.CachePolicy.newBuilder()
                            .setIdleTimeout(Durations.fromSeconds(allocatorVmCacheTimeout.getSeconds()))
                            .build())
                    .build());

        if (op.getDone()) {
            try {
                state.setSessionId(op.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId());
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateSessionResponse", e);
            }
        } else {
            LOG.error(
                "Unexpected create session operation state for: { userId: {}, workflowName: {}, executionId: {} }",
                state.getUserId(), state.getWorkflowName(), state.getExecutionId());
        }

        if (state.getSessionId() == null) {
            state.fail(Status.INTERNAL, "Cannot create allocator session");
        }
    }

    public LongRunning.Operation startAllocation(String dockerImage, String channelManagerAddress, String iamAddress,
                                                 String whiteboardAddress, int portalPort, int slotsApiPort)
    {
        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            privateKey = workerKeys.privateKey();

            var subj = owner.subjectClient.createSubject(AuthProvider.INTERNAL, state.getPortalId(), SubjectType.WORKER,
                new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));

            owner.abClient.setAccessBindings(new Workflow(state.getUserId() + "/" + state.getWorkflowName()),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal, workflow <{}/{}>", state.getUserId(),
                state.getWorkflowName(), e);
            throw new RuntimeException(e);
        }

        var actualPortalPort = (portalPort == -1) ? FreePortFinder.find(10000, 11000) : portalPort;
        var actualSlotsApiPort = (slotsApiPort == -1) ? FreePortFinder.find(11000, 12000) : slotsApiPort;

        var args = new ArrayList<>(List.of(
            "-portal.portal-id=" + state.getPortalId(),
            "-portal.portal-api-port=" + actualPortalPort,
            "-portal.slots-api-port=" + actualSlotsApiPort,
            "-portal.channel-manager-address=" + channelManagerAddress,
            "-portal.iam-address=" + iamAddress,
            "-portal.whiteboard-address=" + whiteboardAddress));

        if (state.getStdoutChannelId() != null) {
            args.add("-portal.stdout-channel-id=" + state.getStdoutChannelId());
        }

        if (state.getStderrChannelId() != null) {
            args.add("-portal.stderr-channel-id=" + state.getStderrChannelId());
        }

        var portalEnvPKEY = "LZY_PORTAL_PKEY";
        var ports = Map.of(actualSlotsApiPort, actualSlotsApiPort, actualPortalPort, actualPortalPort);

        return withIdempotencyKey(owner.allocatorClient, "portal-" + state.getExecutionId()).allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(state.getSessionId())
                .setPoolLabel(cfg.getPoolLabel())
                .setZone(cfg.getPoolZone())
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.SYSTEM)
                .addWorkload(
                    VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                        .setName("portal")
                        .setImage(dockerImage)
                        .addAllArgs(args)
                        .putEnv(portalEnvPKEY, privateKey)
                        .putAllPortBindings(ports)
                        .build())
                .build());
    }

    public void createKafkaTopic(KafkaClient kafkaClient) {
        var topicName = "topic_" + state.getExecutionId() + ".logs";
        var username = "user_" + state.getExecutionId().replace("-", "_");
        var password = UUID.randomUUID().toString();

        LOG.debug("Creating kafka topic {} for execution_id {}", topicName, state.getExecutionId());


        try {
            kafkaClient.createTopic(topicName);
            kafkaClient.createUser(username, password);
            kafkaClient.grantPermission(username, topicName, KafkaClient.TopicRole.PRODUCER);
            kafkaClient.grantPermission(username, topicName, KafkaClient.TopicRole.CONSUMER);

            var topicDesc = new KafkaTopicDesc(username, password, topicName);

            DbHelper.withRetries(LOG, () -> owner.executionDao.setKafkaTopicDesc(state.getExecutionId(),
                topicDesc, null));
        } catch (Exception e) {

            LOG.error("Cannot save kafka topic data in db for execution {}", state.getExecutionId(), e);
            state.fail(Status.INTERNAL, "Cannot create kafka topic");

            try {
                kafkaClient.dropTopic(username);
            } catch (Exception ex) {
                LOG.error("Cannot remove kafka user after error {}: ", e.getMessage(), ex);
            }

            try {
                kafkaClient.dropTopic(topicName);
            } catch (Exception ex) {
                LOG.error("Cannot remove topic after error {}: ", e.getMessage(), ex);
            }

        }
    }
}
