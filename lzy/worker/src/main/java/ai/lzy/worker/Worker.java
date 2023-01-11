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
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.operation.Operation.StdSlotDesc;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.scheduler.SchedulerAgent;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LME;
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
import org.apache.commons.io.input.QueueInputStream;
import org.apache.commons.io.output.QueueOutputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.*;
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
        options.addOption("p", "port", true, "Worker gRPC port.");
        options.addOption("q", "fs-port", true, "LzyFs gRPC port.");
        options.addOption(null, "scheduler-address", true, "Lzy scheduler address [host:port]");
        options.addOption(null, "allocator-address", true, "Lzy allocator address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("i", "iam", true, "Iam address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");
        options.addOption("h", "host", true, "Worker and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "allocator-heartbeat-period", true, "Allocator heartbeat period in duration format");
        options.addOption(null, "scheduler-heartbeat-period", true, "Scheduler heartbeat period in duration format");

        // for tests only
        options.addOption(null, "allocator-token", true, "OTT token for allocator");
    }

    private final LzyFsServer lzyFs;
    private final AllocatorAgent allocatorAgent;
    private final LocalOperationService operationService;
    private final AtomicReference<Environment> env = new AtomicReference<>(null);
    private final AtomicReference<Execution> execution = new AtomicReference<>(null);
    private final Server server;
    private final String schedulerAddress;
    private final Duration schedulerHeartbeatPeriod;
    private final RsaUtils.RsaKeys iamKeys;

    public Worker(
            String vmId, String allocatorAddress, String schedulerAddress, String iamAddress,
            Duration allocatorHeartbeatPeriod, Duration schedulerHeartbeatPeriod, int apiPort, int fsPort,
            String fsRoot, String channelManagerAddress, String host, String allocatorToken)
    {
        LOG.info("Starting worker on vm {}.\n apiPort: {}\n fsPort: {}\n host: {}", vmId, apiPort, fsPort, host);

        this.schedulerAddress = schedulerAddress;
        this.schedulerHeartbeatPeriod = schedulerHeartbeatPeriod;

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
        this.iamKeys = RsaUtils.generateRsaKeys();

        Objects.requireNonNull(allocatorToken);

        operationService = new LocalOperationService(vmId);

        server = newGrpcServer("0.0.0.0", apiPort, GrpcUtils.NO_AUTH)
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
            final var cm = HostAndPort.fromString(channelManagerAddress);
            final var iam = HostAndPort.fromString(iamAddress);

            lzyFs = new LzyFsServer(
                vmId,
                Path.of(fsRoot),
                HostAndPort.fromParts(realHost, fsPort),
                cm,
                iam,
                new RenewableJwt(vmId, "INTERNAL",
                    Duration.ofMinutes(15), readPrivateKey(iamKeys.privateKey())),
                operationService,
                false
            );

            lzyFs.start();
        } catch (IOException e) {
            LOG.error("Error while building uri", e);
            stop();
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        try {
            allocatorAgent = new AllocatorAgent(allocatorToken, vmId, allocatorAddress, allocatorHeartbeatPeriod);
            allocatorAgent.start(Map.of("public-key", iamKeys.publicKey()));
        } catch (AllocatorAgent.RegisterException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Worker inited");
    }

    @VisibleForTesting
    public void stop() {
        LOG.error("Stopping worker");
        server.shutdown();
        allocatorAgent.shutdown();
        if (execution.get() != null) {
            LOG.info("Found current execution, killing it");
            execution.get().signal(Signal.KILL.sig());
        }
        lzyFs.stop();
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

            final var worker = new Worker(
                parse.getOptionValue("vm-id"), parse.getOptionValue("allocator-address"),
                parse.getOptionValue("scheduler-address"), parse.getOptionValue("iam-address"),
                allocHeartbeat == null ? null : Duration.parse(allocHeartbeat),
                Duration.parse(parse.getOptionValue("scheduler-heartbeat-period")),
                Integer.parseInt(parse.getOptionValue('p')), Integer.parseInt(parse.getOptionValue('q')),
                parse.getOptionValue('m'), parse.getOptionValue("channel-manager"), parse.getOptionValue('h'),
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
                Thread.sleep(100);  // Sleep some time to wait for all logs to be written
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    private class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {

        private synchronized void executeOp(ExecuteRequest request,
                                            StreamObserver<ExecuteResponse> response, String opId)
        {
            try (var agent = new SchedulerAgent(
                schedulerAddress, request.getTaskId(), request.getExecutionId(), schedulerHeartbeatPeriod,
                iamKeys.privateKey()))
            {
                String tid = request.getTaskId();
                var task = TaskDesc.fromProto(request.getTaskDesc());

                var lzySlots = new ArrayList<LzySlot>(task.operation().slots().size());

                var errorBindingSlot = Status.INVALID_ARGUMENT.withDescription("Fail to find binding for slot ");

                for (var slot : task.operation().slots()) {
                    final var binding = task.slotsToChannelsAssignments().get(slot.name());
                    if (binding == null) {
                        var errorStatus = errorBindingSlot.withDescription(errorBindingSlot.getDescription() +
                                slot.name());

                        operationService.updateError(opId, errorStatus);
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

                operationService.updateResponse(opId, ExecuteResponse.getDefaultInstance());
                response.onNext(ExecuteResponse.getDefaultInstance());
                response.onCompleted();

                final var stdoutSpec = task.operation().stdout();
                final var stderrSpec = task.operation().stderr();

                final var outQueue = generateStreamQueue(tid, stdoutSpec, "stdout");
                final var errQueue = generateStreamQueue(tid, stderrSpec, "stderr");

                LOG.info("Configuring worker");

                final Environment env = EnvironmentFactory.create(ProtoConverter.fromProto(
                        request.getTaskDesc().getOperation().getEnv()));

                try {
                    env.install(outQueue, errQueue);
                } catch (EnvironmentInstallationException e) {
                    LOG.error("Unable to install environment", e);
                    agent.reportExecutionCompleted(
                        ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(),
                        e.getMessage()
                    );
                    return;
                } catch (Exception e) {
                    LOG.error("Error while preparing env", e);
                    agent.reportExecutionCompleted(
                        ReturnCodes.INTERNAL_ERROR.getRc(),
                        "Error while installing env"
                    );
                    return;
                }

                LOG.info("Executing task");

                try {
                    var exec = new Execution(tid, task.operation().command(), "", lzyFs.getMountPoint().toString());

                    exec.start(env);

                    final int rc = exec.waitFor();

                    agent.reportExecutionCompleted(
                            rc, rc == 0 ? "Success" : "Error while executing command on worker.\n" +
                                "See your stdout/stderr to see more info");
                } catch (Exception e) {
                    LOG.error("Error while executing task, stopping worker", e);
                    agent.reportExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(),
                        "Internal error while executing task");
                }
            }
        }

        private StreamQueue generateStreamQueue(String tid, StdSlotDesc stdoutSpec, String name) {
            final OutputStream stdout;
            if (stdoutSpec != null) {
                stdout = new PipedOutputStream();
                final PipedInputStream i;
                try {
                    i = new PipedInputStream((PipedOutputStream) stdout);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                final var slot = (LineReaderSlot) lzyFs.getSlotsManager().getOrCreateSlot(tid,
                    new TextLinesOutSlot(stdoutSpec.slotName()), stdoutSpec.channelId());
                slot.setStream(new LineNumberReader(new InputStreamReader(
                    i,
                    StandardCharsets.UTF_8
                )));
            } else {
                stdout = OutputStream.nullOutputStream();
            }

            return new StreamQueue(stdout, LOG, name);
        }

        @Override
        public void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> response) {
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

            var op = Operation.create(request.getTaskId(), "Execute worker", idempotencyKey, null);
            OperationSnapshot opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                executeOp(request, response, op.id());
            } else {
                assert idempotencyKey != null;
                LOG.info("Found operation by idempotencyKey: { idempotencyKey: {}, op: {} }", idempotencyKey.token(),
                    opSnapshot.toShortString());

                awaitOpAndReply(opSnapshot.id(), response);
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
