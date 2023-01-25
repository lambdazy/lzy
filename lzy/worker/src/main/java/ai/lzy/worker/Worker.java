package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.OperationSnapshot;
import ai.lzy.longrunning.LocalOperationUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.Signal;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.scheduler.SchedulerAgent;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Configured;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Configured.Err;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Configured.Ok;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Finished;
import ai.lzy.v1.worker.LWS.*;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.env.Environment;
import ai.lzy.worker.env.EnvironmentFactory;
import ai.lzy.worker.env.EnvironmentInstallationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.util.auth.credentials.CredentialsUtils.readPrivateKey;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class Worker {
    private static final Logger LOG = LogManager.getLogger(Worker.class);
    public static boolean USE_LOCALHOST_AS_HOST = false;

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
        options.addOption("h", "host", true, "Worker and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "worker-id", true, "Worker id from scheduler");
        options.addOption(null, "user-default-image", true, "Image, used for inner container by default");
        options.addOption(null, "allocator-heartbeat-period", true, "Allocator heartbeat period in duration format");
        options.addOption(null, "scheduler-heartbeat-period", true, "Scheduler heartbeat period in duration format");

        // for tests only
        options.addOption(null, "allocator-token", true, "OTT token for allocator");
        options.addOption(null, "iam-token", true, "IAM private key for worker");
    }

    private final String workerId;
    private final LzyFsServer lzyFs;
    private final String lzyFsRoot;
    private final SchedulerAgent schedulerAgent;
    private final AllocatorAgent allocatorAgent;
    private final LocalOperationService operationService;
    private final EnvironmentFactory envFactory;
    private final AtomicReference<Environment> env = new AtomicReference<>(null);
    private final AtomicReference<Execution> execution = new AtomicReference<>(null);
    private final Server server;

    public Worker(String workflowName, String workerId, String vmId, String allocatorAddress, String schedulerAddress,
                  Duration allocatorHeartbeatPeriod, Duration schedulerHeartbeatPeriod, int apiPort, int fsPort,
                  String fsRoot, String channelManagerAddress, String host, String defaultUserImage,
                  String iamPrivateKey, String allocatorToken)
    {
        LOG.info("Starting worker on vm {}.\n apiPort: {}\n fsPort: {}\n host: {}", vmId, apiPort, fsPort, host);
        var realHost = host != null ? host : System.getenv(AllocatorAgent.VM_IP_ADDRESS);

        if (realHost == null) {
            if (USE_LOCALHOST_AS_HOST) {
                realHost = "localhost";  // For tests
            } else {
                LOG.error("Cannot resolve host of vm");
                stop();
                throw new RuntimeException("Cannot resolve host of vm");
            }
        }

        allocatorToken = allocatorToken != null ? allocatorToken : System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT);
        iamPrivateKey = iamPrivateKey != null ? iamPrivateKey : System.getenv(ENV_WORKER_PKEY);

        Objects.requireNonNull(allocatorToken);
        Objects.requireNonNull(iamPrivateKey);

        this.workerId = workerId;
        this.envFactory = new EnvironmentFactory(defaultUserImage);

        operationService = new LocalOperationService(workerId);

        server = newGrpcServer("0.0.0.0", apiPort, GrpcUtils.NO_AUTH)
            .addService(new WorkerApiImpl())
            .build();

        try {
            server.start();
        } catch (IOException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }

        try {
            final var fsUri = new URI(LzyFs.scheme(), null, realHost, fsPort, null, null, null);
            final var cm = HostAndPort.fromString(channelManagerAddress);

            lzyFsRoot = fsRoot;
            lzyFs = new LzyFsServer(workerId, Path.of(fsRoot), fsUri, cm, new RenewableJwt(workerId, "INTERNAL",
                Duration.ofDays(1), readPrivateKey(iamPrivateKey)), operationService, false);
            lzyFs.start();
        } catch (IOException | URISyntaxException e) {
            LOG.error("Error while building uri", e);
            server.shutdown();
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
        schedulerAgent = new SchedulerAgent(schedulerAddress, workerId, workflowName, schedulerHeartbeatPeriod,
            HostAndPort.fromParts(realHost, apiPort), iamPrivateKey);

        schedulerAgent.start();
        LOG.info("Worker inited");
    }

    @VisibleForTesting
    public void stop() {
        LOG.error("Stopping worker");
        server.shutdown();
        schedulerAgent.reportStop();
        allocatorAgent.shutdown();
        if (execution.get() != null) {
            LOG.info("Found current execution, killing it");
            execution.get().signal(Signal.KILL.sig());
        }
        schedulerAgent.reportProgress(WorkerProgress.newBuilder()
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
        LOG.info("Starting worker with args {}", Arrays.toString(args));

        final CommandLineParser cliParser = new DefaultParser();
        try {
            final CommandLine parse = cliParser.parse(options, args, true);

            var allocHeartbeat = parse.getOptionValue("allocator-heartbeat-period");

            final var worker = new Worker(parse.getOptionValue('w'), parse.getOptionValue("worker-id"),
                parse.getOptionValue("vm-id"), parse.getOptionValue("allocator-address"),
                parse.getOptionValue("scheduler-address"),
                allocHeartbeat == null ? null : Duration.parse(allocHeartbeat),
                Duration.parse(parse.getOptionValue("scheduler-heartbeat-period")),
                Integer.parseInt(parse.getOptionValue('p')), Integer.parseInt(parse.getOptionValue('q')),
                parse.getOptionValue('m'), parse.getOptionValue("channel-manager"), parse.getOptionValue('h'),
                parse.getOptionValue("user-default-image"), parse.getOptionValue("iam-token"),
                parse.getOptionValue("allocator-token"));

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
        } finally {
            try {
                Thread.sleep(10);  // Sleep some time to wait for all logs to be written
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
            var op = Operation.createCompleted(opId, workerId, "Configure worker", idempotencyKey, null,
                ConfigureResponse.getDefaultInstance());

            OperationSnapshot opSnapshot = operationService.registerOperation(op);

            if (opId.equals(opSnapshot.id())) {
                try {
                    final var e = envFactory.create(lzyFsRoot, ProtoConverter.fromProto(request.getEnv()));
                    schedulerAgent.reportProgress(WorkerProgress.newBuilder()
                        .setConfigured(Configured.newBuilder()
                            .setOk(Ok.newBuilder().build())
                            .build())
                        .build());
                    env.set(e);
                } catch (EnvironmentInstallationException e) {
                    LOG.error("Unable to install environment", e);
                    schedulerAgent.reportProgress(WorkerProgress.newBuilder()
                        .setConfigured(Configured.newBuilder()
                            .setErr(Err.newBuilder()
                                .setDescription(e.getMessage())
                                .build())
                            .build())
                        .build());
                } catch (Exception e) {
                    LOG.error("Error while preparing env, stopping worker", e);
                    schedulerAgent.reportProgress(WorkerProgress.newBuilder()
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
        public synchronized void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> response) {
            if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                LOG.debug("Worker::execute " + JsonUtils.printRequest(request));
            } else {
                LOG.info("Worker::execute request (tid={}, executionId={})",
                    request.getTaskId(), request.getExecutionId());
            }

            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
            if (idempotencyKey != null &&
                loadExistingOpResult(idempotencyKey, response))
            {
                return;
            }

            var op = Operation.create(workerId, "Execute worker", null, idempotencyKey, null);
            OperationSnapshot opSnapshot = operationService.registerOperation(op);

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
                        response.onError(errorStatus.asRuntimeException());

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
                response.onNext(ExecuteResponse.getDefaultInstance());
                response.onCompleted();

                try {
                    var exec = new Execution(tid, task.operation().command(), "", lzyFs.getMountPoint().toString());

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

                    schedulerAgent.reportProgress(WorkerProgress.newBuilder()
                        .setExecutionCompleted(WorkerProgress.ExecutionCompleted.newBuilder()
                            .setRc(rc)
                            .setDescription(rc == 0 ? "Success" : "Error while executing command on worker.\n" +
                                "See your stdout/stderr to see more info")
                            .build())
                        .build());
                    schedulerAgent.reportIdle();
                } catch (Exception e) {
                    LOG.error("Error while executing task, stopping worker", e);
                    Worker.this.stop();
                }
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());

                awaitOpAndReply(opSnapshot.id(), response);
            }
        }

        @Override
        public void stop(StopRequest request, StreamObserver<StopResponse> responseObserver) {
            LOG.info("Stop requested");
            responseObserver.onNext(StopResponse.newBuilder().build());
            responseObserver.onCompleted();

            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            var opId = UUID.randomUUID().toString();
            var op = Operation.createCompleted(opId, workerId, "Stop worker", idempotencyKey, null,
                StopResponse.getDefaultInstance());

            OperationSnapshot opSnapshot = operationService.registerOperation(op);

            if (opId.equals(opSnapshot.id())) {
                Worker.this.stop();
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());
            }
        }

        private boolean loadExistingOpResult(Operation.IdempotencyKey key, StreamObserver<ExecuteResponse> response) {
            return IdempotencyUtils.loadExistingOpResult(operationService, key, ExecuteResponse.class, response,
                "Cannot execute task on worker", LOG);
        }

        private void awaitOpAndReply(String opId, StreamObserver<ExecuteResponse> response) {
            LocalOperationUtils.awaitOpAndReply(operationService, opId, response, ExecuteResponse.class,
                "Cannot execute task on worker", LOG);
        }
    }
}
