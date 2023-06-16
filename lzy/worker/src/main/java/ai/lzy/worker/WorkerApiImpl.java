package ai.lzy.worker;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.logs.LogContextKey;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.StreamQueue.LogHandle;
import ai.lzy.worker.env.AuxEnvironment;
import ai.lzy.worker.env.EnvironmentFactory;
import ai.lzy.worker.env.EnvironmentInstallationException;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static ai.lzy.logs.LogUtils.withLoggingContext;
import static java.util.stream.Collectors.toMap;

@Singleton
public class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkerApiImpl.class);
    public static volatile boolean TEST_ENV = false;

    private final LocalOperationService operationService;
    private final EnvironmentFactory envFactory;
    private final ServiceConfig config;
    private final ManagedChannel channelManagerChannel;
    private final ManagedChannel iamChannel;
    private final KafkaHelper kafkaHelper;

    private record Owner(
        String userId,
        String workflowName
    ) {}

    @Nullable
    private Owner owner = null; // guarded by this
    @Nullable
    private LzyFsServer lzyFs = null; // guarded by this

    private volatile boolean hasActiveExecution = false;

    public WorkerApiImpl(ServiceConfig config, EnvironmentFactory environmentFactory,
                         @Named("WorkerOperationService") LocalOperationService localOperationService,
                         @Named("WorkerChannelManagerGrpcChannel") ManagedChannel channelManagerChannel,
                         @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("WorkerKafkaHelper") KafkaHelper helper)
    {
        this.config = config;
        this.channelManagerChannel = channelManagerChannel;
        this.iamChannel = iamChannel;
        this.kafkaHelper = helper;
        this.operationService = localOperationService;
        this.envFactory = environmentFactory;
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (lzyFs != null) {
            lzyFs.stop();
        }
    }

    private LWS.ExecuteResponse executeOp(LWS.ExecuteRequest request) {
        var tid = request.getTaskId();
        var task = request.getTaskDesc();
        var op = task.getOperation();

        var lzySlots = new ArrayList<LzySlot>(task.getOperation().getSlotsCount());
        var slotAssignments = task.getSlotAssignmentsList().stream()
            .collect(toMap(LMO.SlotToChannelAssignment::getSlotName, LMO.SlotToChannelAssignment::getChannelId));

        for (var slot : op.getSlotsList()) {
            final var binding = slotAssignments.get(slot.getName());
            if (binding == null) {
                LOG.error("Empty binding for slot {}", slot.getName());
                return LWS.ExecuteResponse.newBuilder()
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Internal error")
                    .build();
            }

            lzySlots.add(lzyFs.getSlotsManager().getOrCreateSlot(tid, ProtoConverter.fromProto(slot), binding));
        }

        lzySlots.forEach(slot -> {
            if (slot instanceof LzyFileSlot) {
                lzyFs.addSlot((LzyFileSlot) slot);
            }
        });

        try (final var logHandle = LogHandle.fromTopicDesc(LOG, tid, op.getKafkaTopic(), kafkaHelper)) {
            LOG.info("Configure worker...");

            final AuxEnvironment env;

            try {
                env = envFactory.create(tid, lzyFs.getMountPoint().toString(), op.getEnv(), logHandle);
            } catch (EnvironmentInstallationException e) {
                LOG.error("Unable to install environment", e);

                return LWS.ExecuteResponse.newBuilder()
                    .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                    .setDescription(e.getMessage())
                    .build();
            } catch (Exception e) {
                LOG.error("Error while preparing env", e);
                return LWS.ExecuteResponse.newBuilder()
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Internal error")
                    .build();
            }

            LOG.info("Executing task...");

            try {
                var exec = new Execution(tid, op.getCommand(), "", lzyFs.getMountPoint().toString());

                exec.start(env);

                logHandle.logOut(exec.process().out());
                logHandle.logErr(exec.process().err());

                final int rc = exec.waitFor();
                final String message;

                if (rc == 0) {
                    message = "Success";
                    waitOutputSlots(request, lzySlots);
                } else {
                    message = "Error while executing command on worker. " +
                        "See your stdout/stderr to see more info";
                }

                return LWS.ExecuteResponse.newBuilder()
                    .setRc(rc)
                    .setDescription(message)
                    .build();

            } catch (Exception e) {
                LOG.error("Error while executing task, stopping worker", e);
                return LWS.ExecuteResponse.newBuilder()
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Internal error")
                    .build();
            }
        }
    }

    private void waitOutputSlots(LWS.ExecuteRequest request, ArrayList<LzySlot> lzySlots) throws InterruptedException {
        var outputChannelsIds = lzySlots.stream()
            .filter(slot -> slot.definition().direction() == Slot.Direction.OUTPUT)
            .map(slot -> slot.instance().channelId())
            .collect(Collectors.toSet());

        LOG.info("Task execution successfully completed, wait for OUTPUT slots [{}]...",
            String.join(", ", outputChannelsIds));

        while (!outputChannelsIds.isEmpty()) {
            var outputChannels = lzyFs.getSlotsManager()
                .getChannelsStatus(request.getExecutionId(), outputChannelsIds);

            if (outputChannels == null) {
                LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
                continue;
            }

            if (outputChannels.isEmpty()) {
                LOG.warn("We don't have any information about channels, just wait a little...");
                LockSupport.parkNanos(Duration.ofSeconds(5).toNanos());
                break;
            }

            for (var channel : outputChannels) {
                if (!channel.hasSpec()) {
                    LOG.warn("Channel {} not found, maybe it's not ALIVE", channel.getChannelId());
                    outputChannelsIds.remove(channel.getChannelId());
                    continue;
                }

                if (channel.getSenders().hasPortalSlot()) {
                    LOG.info("Channel {} has portal in senders", channel.getChannelId());
                    outputChannelsIds.remove(channel.getChannelId());
                } else if (TEST_ENV) {
                    var slotName = channel.getSenders().getWorkerSlot().getSlot().getName();
                    var slot = (LzyOutputSlot) lzySlots.stream()
                        .filter(s -> s.name().equals(slotName))
                        .findFirst()
                        .get();

                    var readers = channel.getReceivers().getWorkerSlotsCount() +
                        (channel.getReceivers().hasPortalSlot() ? 1 : 0);

                    if (slot.getCompletedReads() >= readers) {
                        LOG.info("Channel {} already read ({}) by all consumers ({})",
                            channel.getChannelId(), slot.getCompletedReads(), readers);
                        outputChannelsIds.remove(channel.getChannelId());
                    } else {
                        LOG.info(
                            "Channel {} neither has portal in senders nor completely read, wait...",
                            channel.getChannelId());
                    }
                }
            }

            if (!outputChannelsIds.isEmpty()) {
                LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
            }
        }
    }

    public synchronized LzyFsServer lzyFs() {
        return Objects.requireNonNull(lzyFs);
    }

    @Override
    public synchronized void init(LWS.InitRequest request, StreamObserver<LWS.InitResponse> response) {
        var owner = this.owner;
        var requester = new Owner(request.getUserId(), request.getWorkflowName());

        if (owner == null) {
            this.owner = requester;

            RenewableJwt token;
            try {
                token = new RenewableJwt(
                    request.getWorkerSubjectName(),
                    AuthProvider.INTERNAL.name(),
                    Duration.ofDays(1),
                    CredentialsUtils.readPrivateKey(request.getWorkerPrivateKey()));
            } catch (Exception e) {
                LOG.error("Cannot create renewable JWT token: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.asException());
                return;
            }

            lzyFs = new LzyFsServer(
                config.getVmId(),
                Path.of(config.getMountPoint()),
                HostAndPort.fromParts(config.getHost(), config.getFsPort()),
                channelManagerChannel,
                iamChannel,
                token,
                operationService,
                /* isPortal */ false
            );

            try {
                lzyFs.start();
            } catch (IOException e) {
                LOG.error("Cannot start LzyFs server", e);
                response.onError(Status.INTERNAL
                    .withDescription("Cannot start LzyFs server: " + e.getMessage()).asException());
                return;
            }

            var workflow = new Workflow(requester.userId() + '/' + requester.workflowName());
            lzyFs.configureAccess(workflow, AuthPermission.WORKFLOW_RUN);
        } else if (!owner.equals(requester)) {
            LOG.error("Attempt to execute op from another owner. Current is {}, got {}", owner, requester);
            response.onError(Status.PERMISSION_DENIED.asException());
            return;
        } else {
            LOG.info("Attempt to reuse worker {} for another execution of workflow {}, has active execution: {}",
                config.getVmId(), request.getWorkflowName(), hasActiveExecution);
            while (hasActiveExecution) {
                LOG.warn("Worker still has active execution, wait...");
                LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
            }
        }

        response.onNext(LWS.InitResponse.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public synchronized void execute(LWS.ExecuteRequest request, StreamObserver<LongRunning.Operation> response) {
        withLoggingContext(
            Map.of(
                "tid", request.getTaskId(),
                LogContextKey.EXECUTION_ID, request.getExecutionId()),
            () -> {
                if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                    LOG.debug("Worker::execute {}", ProtoPrinter.safePrinter().printToString(request));
                } else {
                    LOG.info("Worker::execute request");
                }

                var requester = new Owner(request.getUserId(), request.getWorkflowName());
                if (owner == null || !owner.equals(requester)) {
                    LOG.error("Attempt to execute op from another owner. Current is {}, got {}", owner, requester);
                    response.onError(Status.PERMISSION_DENIED.asException());
                    return;
                }

                var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
                if (idempotencyKey != null && loadExistingOpResult(idempotencyKey, response)) {
                    return;
                }

                var op = Operation.create(
                    request.getTaskId(),
                    "Worker, executionId: " + request.getExecutionId() + ", taskId: " + request.getTaskId(),
                    /* deadline */ null,
                    idempotencyKey,
                    /* meta */ null);

                hasActiveExecution = true;
                var opSnapshot = operationService.execute(op, () -> {
                    try {
                        return executeOp(request);
                    } finally {
                        hasActiveExecution = false;
                    }
                });

                response.onNext(opSnapshot.toProto());
                response.onCompleted();
            });
    }

    private boolean loadExistingOpResult(Operation.IdempotencyKey key,
                                         StreamObserver<LongRunning.Operation> response)
    {
        return IdempotencyUtils.loadExistingOp(operationService, key, response, LOG);
    }
}
