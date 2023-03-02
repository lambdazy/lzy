package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.logs.StreamQueue;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.OperationSnapshot;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMO.Operation.StdSlotDesc;
import ai.lzy.v1.common.LMO.SlotToChannelAssignment;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS.ExecuteRequest;
import ai.lzy.v1.worker.LWS.ExecuteResponse;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.env.AuxEnvironment;
import ai.lzy.worker.env.EnvironmentFactory;
import ai.lzy.worker.env.EnvironmentInstallationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class Worker {
    private static final Logger LOG = LogManager.getLogger(Worker.class);
    private static final int DEFAULT_FS_PORT = 9876;
    private static final int DEFAULT_API_PORT = 9877;
    private static final AtomicBoolean USE_LOCALHOST_AS_HOST = new AtomicBoolean(false);
    private static final AtomicBoolean SELECT_RANDOM_VALUES = new AtomicBoolean(false);
    private static RsaUtils.RsaKeys RSA_KEYS = null;  // only for tests

    private static final Options options = new Options();

    static {
        options.addOption(null, "allocator-address", true, "Lzy allocator address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("i", "iam", true, "Iam address [host:port]");
        options.addOption("h", "host", true, "Worker and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "allocator-heartbeat-period", true, "Allocator heartbeat period in duration format");

        // for tests only
        options.addOption(null, "allocator-token", true, "OTT token for allocator");
    }

    private final LzyFsServer lzyFs;
    private final String lzyFsRoot;
    private final AllocatorAgent allocatorAgent;
    private final LocalOperationService operationService;
    private final AtomicReference<Execution> execution = new AtomicReference<>(null);
    private final EnvironmentFactory envFactory;
    private final Server server;
    private final ServiceConfig config;

    public Worker(ServiceConfig config) {
        this.config = config;

        LOG.info("Starting worker on vm {}.\n apiPort: {}\n fsPort: {}\n host: {}", config.getVmId(),
            config.getApiPort(), config.getFsPort(), config.getHost());

        var allocatorToken = config.getAllocatorToken() != null
            ? config.getAllocatorToken()
            : System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT);

        Objects.requireNonNull(allocatorToken);

        operationService = new LocalOperationService(config.getVmId());

        String gpuCount = System.getenv(AllocatorAgent.VM_GPU_COUNT);
        this.envFactory = new EnvironmentFactory(gpuCount == null ? 0 : Integer.parseInt(gpuCount));

        server = newGrpcServer("0.0.0.0", config.getApiPort(), GrpcUtils.NO_AUTH)
            .addService(new WorkerApiImpl())
            .addService(operationService)
            .build();

        try {
            server.start();
        } catch (IOException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }

        try {
            final var cm = HostAndPort.fromString(config.getChannelManagerAddress());
            final var iam = HostAndPort.fromString(config.getIam().getAddress());

            lzyFsRoot = config.getMountPoint();
            lzyFs = new LzyFsServer(
                config.getVmId(),
                Path.of(config.getMountPoint()),
                HostAndPort.fromParts(config.getHost(), config.getFsPort()),
                cm,
                iam,
                config.getIam().createRenewableToken(),
                operationService,
                false
            );
            lzyFs.start();
        } catch (IOException e) {
            LOG.error("Error while building uri", e);
            server.shutdown();
            throw new RuntimeException(e);
        }

        try {
            allocatorAgent = new AllocatorAgent(allocatorToken, config.getVmId(), config.getAllocatorAddress(),
                config.getAllocatorHeartbeatPeriod());
            allocatorAgent.start(Map.of(
                MetadataConstants.PUBLIC_KEY, config.getPublicKey(),
                MetadataConstants.API_PORT, String.valueOf(config.getApiPort()),
                MetadataConstants.FS_PORT, String.valueOf(config.getFsPort())
            ));
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
        LOG.info("Starting worker with args [{}]", Arrays.stream(args)
            .map(s -> s.startsWith("-----BEGIN RSA PRIVATE KEY-----") ? "<rsa-private-key>" : s)
            .collect(Collectors.joining(", ")));

        final CommandLineParser cliParser = new DefaultParser();
        try {
            final CommandLine parse = cliParser.parse(options, args, true);

            var allocHeartbeat = parse.getOptionValue("allocator-heartbeat-period");

            var vmId = parse.getOptionValue("vm-id");
            vmId = vmId == null ? System.getenv(AllocatorAgent.VM_ID_KEY) : vmId;

            var allocatorAddress = parse.getOptionValue("allocator-address");
            var allocHeartbeatDur = allocHeartbeat == null ? null : Duration.parse(allocHeartbeat);
            var iamAddress = parse.getOptionValue("iam");
            var channelManagerAddress = parse.getOptionValue("channel-manager");
            var host = parse.getOptionValue('h');
            var allocatorToken = parse.getOptionValue("allocator-token");

            var worker = startWorker(vmId, allocatorAddress, iamAddress, allocHeartbeatDur, channelManagerAddress, host,
                allocatorToken);

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
                // ignored
            }
        }
    }

    @VisibleForTesting
    public static Worker startWorker(String vmId, String allocatorAddress, String iamAddress,
                                     Duration allocatorHeartbeatPeriod,
                                     String channelManagerAddress, String host, String allocatorToken)
    {
        var realHost = host != null ? host : System.getenv(AllocatorAgent.VM_IP_ADDRESS);

        final int fsPort;
        final String fsRoot;
        final int apiPort;

        if (SELECT_RANDOM_VALUES.get()) {
            fsPort = FreePortFinder.find(10000, 20000);
            apiPort = FreePortFinder.find(20000, 30000);
            fsRoot = "/tmp/lzy" + UUID.randomUUID();
        } else {
            fsPort = DEFAULT_FS_PORT;
            apiPort = DEFAULT_API_PORT;
            fsRoot = "/tmp/lzy";
        }

        if (realHost == null) {
            if (USE_LOCALHOST_AS_HOST.get()) {
                realHost = "localhost";  // For tests
            } else {
                LOG.error("Cannot resolve host of vm");
                throw new RuntimeException("Cannot resolve host of vm");
            }
        }

        final RsaUtils.RsaKeys iamKeys;

        if (RSA_KEYS == null) {
            try {
                iamKeys = RsaUtils.generateRsaKeys();
            } catch (IOException | InterruptedException e) {
                LOG.error("Cannot generate keys");
                throw new RuntimeException(e);
            }
        } else {
            iamKeys = RSA_KEYS;
        }

        var properties = new HashMap<String, Object>(Map.of(
            "worker.vm-id", vmId,
            "worker.allocator-address", allocatorAddress,
            "worker.channel-manager-address", channelManagerAddress,
            "worker.host", realHost,
            "worker.allocator-token", allocatorToken,
            "worker.fs-port", fsPort,
            "worker.api-port", apiPort,
            "worker.mount-point", fsRoot,
            "worker.iam.address", iamAddress,
            "worker.iam.internal-user-name", vmId
        ));

        properties.put("worker.iam.internal-user-private-key", iamKeys.privateKey());
        properties.put("worker.public-key", iamKeys.publicKey());
        properties.put("worker.allocator-heartbeat-period", allocatorHeartbeatPeriod);

        var ctx = ApplicationContext.run(properties);
        return ctx.getBean(Worker.class);
    }

    @VisibleForTesting
    public static void setRsaKeysForTests(@Nullable RsaUtils.RsaKeys keys) {
        RSA_KEYS = keys;
    }

    @VisibleForTesting
    public static void selectRandomValues(boolean val) {
        SELECT_RANDOM_VALUES.set(val);
    }

    @VisibleForTesting
    public static void useLocalhostAsHost(boolean use) {
        USE_LOCALHOST_AS_HOST.set(use);
    }

    @VisibleForTesting
    public int apiPort() {
        return config.getApiPort();
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    private class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {

        private synchronized ExecuteResponse executeOp(ExecuteRequest request) {
            String tid = request.getTaskId();
            var task = request.getTaskDesc();
            var op = task.getOperation();

            var lzySlots = new ArrayList<LzySlot>(task.getOperation().getSlotsCount());
            var slotAssignments = task.getSlotAssignmentsList().stream()
                .collect(Collectors.toMap(SlotToChannelAssignment::getSlotName, SlotToChannelAssignment::getChannelId));

            for (var slot : op.getSlotsList()) {
                final var binding = slotAssignments.get(slot.getName());
                if (binding == null) {
                    return ExecuteResponse.newBuilder()
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

            try (final var logHandle = StreamQueue.LogHandle.fromHeaders(LOG)) {

                final var stdoutSpec = op.hasStdout() ? op.getStdout() : null;
                final var stderrSpec = op.hasStderr() ? op.getStderr() : null;
                logHandle.addErrOutput(generateStdOutputStream(tid, stderrSpec));
                logHandle.addOutOutput(generateStdOutputStream(tid, stdoutSpec));

                LOG.info("Configuring worker");

                final AuxEnvironment env = envFactory.create(lzyFsRoot, request.getTaskDesc().getOperation().getEnv());

                try {
                    env.base().install(logHandle);
                    env.install(logHandle);
                } catch (EnvironmentInstallationException e) {
                    LOG.error("Unable to install environment", e);

                    return ExecuteResponse.newBuilder()
                        .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                        .setDescription(e.getMessage())
                        .build();
                } catch (Exception e) {
                    LOG.error("Error while preparing env", e);
                    return ExecuteResponse.newBuilder()
                        .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                        .setDescription("Internal error")
                        .build();
                }

                LOG.info("Executing task {}", tid);

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

                    return ExecuteResponse.newBuilder()
                        .setRc(rc)
                        .setDescription(message)
                        .build();

                } catch (Exception e) {
                    LOG.error("Error while executing task, stopping worker", e);
                    return ExecuteResponse.newBuilder()
                        .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                        .setDescription("Internal error")
                        .build();
                }
            }
        }

        private void waitOutputSlots(ExecuteRequest request, ArrayList<LzySlot> lzySlots) throws InterruptedException {
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
                    Thread.sleep(Duration.ofSeconds(1).toMillis());
                    continue;
                }

                if (outputChannels.isEmpty()) {
                    LOG.warn("We don't have any information about channels, just wait a little...");
                    LockSupport.parkNanos(Duration.ofSeconds(30).toNanos());
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
                    } else {
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
                    Thread.sleep(Duration.ofSeconds(1).toMillis());
                }
            }
        }

        private OutputStream generateStdOutputStream(String tid, @Nullable StdSlotDesc stdoutSpec) {
            final OutputStream stdout;
            if (stdoutSpec != null) {
                stdout = new PipedOutputStream();

                final PipedInputStream i;
                try {
                    i = new PipedInputStream((PipedOutputStream) stdout);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                final var slot = (LineReaderSlot) lzyFs.getSlotsManager().getOrCreateSlot(
                    tid,
                    new TextLinesOutSlot(stdoutSpec.getName()),
                    stdoutSpec.getChannelId());

                slot.setStream(new LineNumberReader(new InputStreamReader(i, StandardCharsets.UTF_8)));
            } else {
                stdout = OutputStream.nullOutputStream();
            }

            return stdout;
        }

        @Override
        public void execute(ExecuteRequest request, StreamObserver<LongRunning.Operation> response) {
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

            var op = Operation.create(request.getTaskId(), "Execute worker", null, idempotencyKey, null);

            OperationSnapshot opSnapshot = operationService.execute(op, () -> executeOp(request));

            response.onNext(opSnapshot.toProto());
            response.onCompleted();
        }

        private boolean loadExistingOpResult(Operation.IdempotencyKey key,
                                             StreamObserver<LongRunning.Operation> response)
        {
            return IdempotencyUtils.loadExistingOp(operationService, key, response, LOG);
        }
    }
}
