package ai.lzy.worker;

import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.Execution;
import ai.lzy.env.aux.AuxEnvironment;
import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.Logs;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.logs.LogContextKey;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.slots.Slots;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.logs.LogUtils.withLoggingContext;
import static ai.lzy.worker.LogConstants.STDERR;
import static ai.lzy.worker.LogConstants.STDOUT;
import static java.util.stream.Collectors.toMap;

@Singleton
public class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkerApiImpl.class);
    public static volatile boolean TEST_ENV = false;

    private final LocalOperationService operationService;
    private final EnvironmentFactory envFactory;
    private final ServiceConfig config;
    private final ManagedChannel iamChannel;
    private final AccessServerInterceptor internalOnly;
    private final KafkaHelper kafkaHelper;

    private record Owner(
        String userId,
        String workflowName
    ) {}

    @Nullable
    private Owner owner = null; // guarded by this
    @Nullable
    private Slots slots = null; // guarded by this

    private volatile boolean hasActiveExecution = false;

    public WorkerApiImpl(ServiceConfig config, EnvironmentFactory environmentFactory,
                         @Named("WorkerOperationService") LocalOperationService localOperationService,
                         @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("WorkerInternalOnlyInterceptor") AccessServerInterceptor internalOnly,
                         @Named("WorkerKafkaHelper") KafkaHelper helper)
    {
        this.config = config;
        this.iamChannel = iamChannel;
        this.internalOnly = internalOnly;
        this.kafkaHelper = helper;
        this.operationService = localOperationService;
        this.envFactory = environmentFactory;
    }

    @Override
    public synchronized void execute(LWS.ExecuteRequest request, StreamObserver<LongRunning.Operation> response) {
        var logContextOverrides = Map.of(
            LogContextKey.EXECUTION_ID, request.getExecutionId(),
            "tid", request.getTaskId());

        var grpcHeadersOverrides = Map.of(
            GrpcHeaders.X_EXECUTION_ID, request.getExecutionId(),
            GrpcHeaders.X_EXECUTION_TASK_ID, request.getTaskId());

        withLoggingContext(
            logContextOverrides,
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

                if (slots == null) {
                    LOG.error("Unexpected error");
                    response.onError(Status.INTERNAL.asException());
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
                }, logContextOverrides, grpcHeadersOverrides);

                response.onNext(opSnapshot.toProto());
                response.onCompleted();
            });
    }

    private LWS.ExecuteResponse executeOp(LWS.ExecuteRequest request) {
        var tid = request.getTaskId();
        var task = request.getTaskDesc();
        var op = task.getOperation();

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
        }

        final var logs = Logs.builder()
            .withWriters(new KafkaLogsWriter(op.getKafkaTopic(), LOG, tid, kafkaHelper))
            .withCollections(LogConstants.LOGS)
            .build();

        try (logs) {
            LOG.info("Configure worker...");

            final AuxEnvironment env;

            try {
                env = envFactory.create(config.getMountPoint(), op.getEnv(), config.getMountPoint());
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

            var slotsContext = slots.context("", request.getExecutionId(), tid, op.getSlotsList(), slotAssignments);

            try {
                var exec = new Execution(op.getCommand(), "");

                LOG.info("Waiting for slots...");
                slotsContext.beforeExecution();

                exec.start(env);

                STDOUT.log(exec.process().out());
                STDERR.log(exec.process().err());

                final int rc = exec.waitFor();
                final String message;

                if (rc == 0) {
                    message = "Success";
                    slotsContext.afterExecution();
                } else {
                    message = "Error while executing command on worker. See your stdout/stderr to see more info.";
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

            try {
                internalOnly.configureToken(token::get);
                internalOnly.configure(
                    new Workflow(requester.userId() + "/" + request.getWorkflowName()), AuthPermission.WORKFLOW_MANAGE
                );
                slots = new Slots(
                    Path.of(config.getMountPoint()),
                    () -> token.get().token(),
                    HostAndPort.fromParts(config.getHost(), config.getFsPort()),
                    HostAndPort.fromString(config.getChannelManagerAddress()),
                    config.getVmId(),
                    iamChannel,
                    request.getWorkflowName(),
                    requester.userId()
                );
            } catch (IOException e) {
                LOG.error("Cannot start slots server", e);
                response.onError(Status.INTERNAL
                    .withDescription("Cannot start slots server: " + e.getMessage()).asException());
                return;
            }
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

    @PreDestroy
    public void close() {
        if (slots != null) {
            slots.close();
        }
    }

    private boolean loadExistingOpResult(Operation.IdempotencyKey key,
                                         StreamObserver<LongRunning.Operation> response)
    {
        return IdempotencyUtils.loadExistingOp(operationService, key, response, LOG);
    }
}
