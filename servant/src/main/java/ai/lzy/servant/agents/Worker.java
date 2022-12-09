package ai.lzy.servant.agents;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.ImmutableCopyOperation;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.EnvironmentInstallationException;
import ai.lzy.model.Signal;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.scheduler.SchedulerAgent;
import ai.lzy.servant.env.Environment;
import ai.lzy.servant.env.EnvironmentFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Configured;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Configured.Err;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Configured.Ok;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Finished;
import ai.lzy.v1.worker.LWS.*;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.util.auth.credentials.CredentialsUtils.readPrivateKey;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class Worker {
    private static final Logger LOG = LogManager.getLogger(Worker.class);

    public static final String ENV_WORKER_PKEY = "LZY_WORKER_PKEY";

    private static final Options options = new Options();

    static {
        options.addOption("w", "workflow-name", true, "Workflow name");
        options.addOption("p", "port", true, "Worker gRPC port.");
        options.addOption("q", "fs-port", true, "LzyFs gRPC port.");
        options.addOption(null, "scheduler-address", true, "Lzy scheduler address [host:port]");
        options.addOption(null, "allocator-address", true, "Lzy allocator address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");
        options.addOption("h", "host", true, "Servant and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "servant-id", true, "Servant id from scheduler");
        options.addOption(null, "allocator-heartbeat-period", true, "Allocator heartbeat period in duration format");
        options.addOption(null, "scheduler-heartbeat-period", true, "Scheduler heartbeat period in duration format");

        // for tests only
        options.addOption(null, "allocator-token", true, "OTT token for allocator");
        options.addOption(null, "iam-token", true, "IAM private key for servant");
    }

    private final String servantId;
    private final LzyFsServer lzyFs;
    private final SchedulerAgent schedulerAgent;
    private final AllocatorAgent allocatorAgent;
    private final LocalOperationService operationService;
    private final AtomicReference<Environment> env = new AtomicReference<>(null);
    private final AtomicReference<LzyExecution> execution = new AtomicReference<>(null);
    private final Server server;

    public Worker(String workflowName, String servantId, String vmId, String allocatorAddress, String schedulerAddress,
                  Duration allocatorHeartbeatPeriod, Duration schedulerHeartbeatPeriod, int apiPort, int fsPort,
                  String fsRoot, String channelManagerAddress, String host, String iamPrivateKey, String allocatorToken)
    {
        final var realHost = host != null ? host : System.getenv(AllocatorAgent.VM_IP_ADDRESS);

        allocatorToken = allocatorToken != null ? allocatorToken : System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT);
        iamPrivateKey = iamPrivateKey != null ? iamPrivateKey : System.getenv(ENV_WORKER_PKEY);

        Objects.requireNonNull(allocatorToken);
        Objects.requireNonNull(iamPrivateKey);

        this.servantId = servantId;

        operationService = new LocalOperationService(servantId);

        server = newGrpcServer(realHost, apiPort, GrpcUtils.NO_AUTH)
            .addService(new WorkerApiImpl())
            .build();

        try {
            server.start();
        } catch (IOException e) {
            LOG.error(e);
            stop();
            throw new RuntimeException(e);
        }

        try {
            final var fsUri = new URI(LzyFs.scheme(), null, host, fsPort, null, null, null);
            final var cm = HostAndPort.fromString(channelManagerAddress);

            lzyFs = new LzyFsServer(servantId, Path.of(fsRoot), fsUri, cm, new RenewableJwt(servantId, "INTERNAL",
                Duration.ofDays(1), readPrivateKey(iamPrivateKey)), operationService);
            lzyFs.start();
        } catch (IOException | URISyntaxException e) {
            LOG.error("Error while building uri", e);
            stop();
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        try {
            allocatorAgent = new AllocatorAgent(allocatorToken, vmId, allocatorAddress, allocatorHeartbeatPeriod);
            allocatorAgent.start();
        } catch (AllocatorAgent.RegisterException e) {
            throw new RuntimeException(e);
        }

        // TODO: when should we start this agent?
        //       we can share VM among _all_ workflows of user, so, workflowName is not a constant
        schedulerAgent = new SchedulerAgent(schedulerAddress, servantId, workflowName, schedulerHeartbeatPeriod,
            apiPort, iamPrivateKey);

        schedulerAgent.start();
    }

    @VisibleForTesting
    public void stop() {
        LOG.error("Stopping servant");
        server.shutdown();
        schedulerAgent.reportStop();
        allocatorAgent.shutdown();
        if (execution.get() != null) {
            LOG.info("Found current execution, killing it");
            execution.get().signal(Signal.KILL.sig());
        }
        schedulerAgent.reportProgress(ServantProgress.newBuilder()
            .setFinished(Finished.newBuilder().build())
            .build());
        lzyFs.stop();
        schedulerAgent.shutdown();
    }

    private void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @VisibleForTesting
    public static int execute(String[] args) {
        final CommandLineParser cliParser = new DefaultParser();
        try {
            final CommandLine parse = cliParser.parse(options, args, true);

            final var worker = new Worker(parse.getOptionValue('w'), parse.getOptionValue("servant-id"),
                parse.getOptionValue("vm-id"), parse.getOptionValue("allocator-address"),
                parse.getOptionValue("scheduler-address"),
                Duration.parse(parse.getOptionValue("allocator-heartbeat-period")),
                Duration.parse(parse.getOptionValue("scheduler-heartbeat-period")),
                Integer.parseInt(parse.getOptionValue('p')), Integer.parseInt(parse.getOptionValue('q')),
                parse.getOptionValue('m'), parse.getOptionValue("channel-manager"), parse.getOptionValue('h'),
                parse.getOptionValue("iam-token"), parse.getOptionValue("allocator-token"));

            try {
                worker.awaitTermination();
            } catch (InterruptedException e) {
                worker.stop();
            }
            return 0;
        } catch (ParseException e) {
            LOG.error("Cannot correctly parse options", e);
            return -1;
        } catch (Exception e) {
            LOG.error("Error while executing: " + String.join(" ", args), e);
            return -1;
        }
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    private class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {

        @Override
        public synchronized void configure(ConfigureRequest request, StreamObserver<ConfigureResponse> response) {
            LOG.info("Configuring worker");
            response.onNext(ConfigureResponse.getDefaultInstance());
            response.onCompleted();

            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            var opId = UUID.randomUUID().toString();
            var op = Operation.createCompleted(opId, servantId, "Configure worker", idempotencyKey, null,
                ConfigureResponse.getDefaultInstance());

            ImmutableCopyOperation opSnapshot = operationService.registerOperation(op);

            if (opId.equals(opSnapshot.id())) {
                try {
                    final var e = EnvironmentFactory.create(ProtoConverter.fromProto(request.getEnv()));
                    schedulerAgent.reportProgress(ServantProgress.newBuilder()
                        .setConfigured(Configured.newBuilder()
                            .setOk(Ok.newBuilder().build())
                            .build())
                        .build());
                    env.set(e);
                } catch (EnvironmentInstallationException e) {
                    LOG.error("Unable to install environment", e);
                    schedulerAgent.reportProgress(ServantProgress.newBuilder()
                        .setConfigured(Configured.newBuilder()
                            .setErr(Err.newBuilder()
                                .setDescription(e.getMessage())
                                .build())
                            .build())
                        .build());
                } catch (Exception e) {
                    LOG.error("Error while preparing env, stopping servant", e);
                    schedulerAgent.reportProgress(ServantProgress.newBuilder()
                        .setConfigured(Configured.newBuilder()
                            .setErr(Err.newBuilder()
                                .setDescription("Internal exception")
                                .build())
                            .build())
                        .build());
                    Worker.this.stop();
                }
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());
            }
        }

        @Override
        public synchronized void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> responseObserver) {
            if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                LOG.debug("Worker::execute " + JsonUtils.printRequest(request));
            } else {
                LOG.info("Worker::execute request (tid={}, executionId={})",
                    request.getTaskId(), request.getExecutionId());
            }

            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
            if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey, ExecuteResponse.class,
                responseObserver, "Cannot execute task on worker", LOG))
            {
                return;
            }

            var op = Operation.create(servantId, "Execute worker", idempotencyKey, null);
            ImmutableCopyOperation opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                String tid = request.getTaskId();
                var task = TaskDesc.fromProto(request.getTaskDesc());

                var lzySlots = new ArrayList<LzySlot>(task.operation().slots().size());

                var errorBindingSlot = Status.INVALID_ARGUMENT.withDescription("Fail to find binding for slot ");

                for (var slot : task.operation().slots()) {
                    final var binding = task.slotsToChannelsAssignments().get(slot.name());
                    if (binding == null) {
                        var errorStatus = errorBindingSlot.withDescription(errorBindingSlot.getDescription() +
                            slot.name());

                        operationService.updateError(op.id(), errorStatus);
                        responseObserver.onError(errorStatus.asRuntimeException());

                        return;
                    }

                    lzySlots.add(lzyFs.getSlotsManager().getOrCreateSlot(tid, slot, binding));
                }

                lzySlots.forEach(slot -> {
                    if (slot instanceof LzyFileSlot) {
                        lzyFs.addSlot((LzyFileSlot) slot);
                    }
                });

                operationService.updateResponse(op.id(), ExecuteResponse.getDefaultInstance());
                responseObserver.onNext(ExecuteResponse.getDefaultInstance());
                responseObserver.onCompleted();

                try {
                    var exec = new LzyExecution(tid, task.operation().command(), "", lzyFs.getMountPoint().toString());

                    exec.start(env.get());
                    schedulerAgent.reportExecuting();

                    final var stdoutSpec = task.operation().stdout();
                    final var stderrSpec = task.operation().stderr();

                    if (stdoutSpec != null) {
                        final var slot = (LineReaderSlot) lzyFs.getSlotsManager().getOrCreateSlot(tid,
                            new TextLinesOutSlot(stdoutSpec.slotName()), stdoutSpec.channelId());
                        slot.setStream(new LineNumberReader(new InputStreamReader(
                            exec.process().out(),
                            StandardCharsets.UTF_8
                        )));
                    }

                    if (stderrSpec != null) {
                        final var slot = (LineReaderSlot) lzyFs.getSlotsManager().getOrCreateSlot(tid,
                            new TextLinesOutSlot(stderrSpec.slotName()), stderrSpec.channelId());
                        slot.setStream(new LineNumberReader(new InputStreamReader(
                            exec.process().err(),
                            StandardCharsets.UTF_8
                        )));
                    }

                    final int rc = exec.waitFor();

                    schedulerAgent.reportProgress(ServantProgress.newBuilder()
                        .setExecutionCompleted(ServantProgress.ExecutionCompleted.newBuilder()
                            .setRc(rc)
                            .setDescription(rc == 0 ? "Success" : "Error while executing command on worker.\n" +
                                "See your stdout/stderr to see more info")
                            .build())
                        .build());
                    schedulerAgent.reportIdle();
                } catch (Exception e) {
                    LOG.error("Error while executing task, stopping servant", e);
                    Worker.this.stop();
                }
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());

                var typeResp = ExecuteResponse.class;
                var internalErrorMessage = "Cannot execute task on worker";

                var opId = opSnapshot.id();

                opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50),
                    Duration.ofSeconds(5));

                if (opSnapshot == null) {
                    LOG.error("Can not find operation with id: { opId: {} }", opId);
                    responseObserver.onError(Status.INTERNAL.asRuntimeException());
                    return;
                }

                if (opSnapshot.done()) {
                    if (opSnapshot.response() != null) {
                        try {
                            var resp = opSnapshot.response().unpack(typeResp);
                            responseObserver.onNext(resp);
                            responseObserver.onCompleted();
                        } catch (InvalidProtocolBufferException e) {
                            LOG.error("Error while waiting op result: {}", e.getMessage(), e);
                            responseObserver.onError(Status.INTERNAL.asRuntimeException());
                        }
                    } else {
                        var error = opSnapshot.error();
                        assert error != null;
                        responseObserver.onError(error.asRuntimeException());
                    }
                } else {
                    LOG.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
                    responseObserver.onError(
                        Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
                }
            }
        }

        @Override
        public void stop(StopRequest request, StreamObserver<StopResponse> responseObserver) {
            LOG.info("Stop requested");
            responseObserver.onNext(StopResponse.newBuilder().build());
            responseObserver.onCompleted();

            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            var opId = UUID.randomUUID().toString();
            var op = Operation.createCompleted(opId, servantId, "Stop worker", idempotencyKey, null,
                StopResponse.getDefaultInstance());

            ImmutableCopyOperation opSnapshot = operationService.registerOperation(op);

            if (opId.equals(opSnapshot.id())) {
                Worker.this.stop();
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());
            }
        }
    }
}
