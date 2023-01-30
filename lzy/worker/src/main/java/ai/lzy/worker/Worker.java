package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.OperationSnapshot;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.operation.Operation.StdSlotDesc;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS.*;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.env.Environment;
import ai.lzy.worker.env.EnvironmentFactory;
import ai.lzy.worker.env.EnvironmentInstallationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
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
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.util.auth.credentials.CredentialsUtils.readPrivateKey;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class Worker {
    private static final Logger LOG = LogManager.getLogger(Worker.class);
    private static boolean USE_LOCALHOST_AS_HOST = false;
    private static RsaUtils.RsaKeys RSA_KEYS = null;  // only for tests

    private static final Options options = new Options();

    static {
        options.addOption("p", "port", true, "Worker gRPC port.");
        options.addOption("q", "fs-port", true, "LzyFs gRPC port.");
        options.addOption(null, "allocator-address", true, "Lzy allocator address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("i", "iam", true, "Iam address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");
        options.addOption("h", "host", true, "Worker and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "user-default-image", true, "Image, used for inner container by default");
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

    public Worker(String vmId, String allocatorAddress, String iamAddress, Duration allocatorHeartbeatPeriod,
                  int apiPort, int fsPort, String fsRoot, String channelManagerAddress, String host,
                  String allocatorToken, String defaultUserImage)
    {
        var realHost = host != null ? host : System.getenv(AllocatorAgent.VM_IP_ADDRESS);

        LOG.info("Starting worker on vm {}.\n apiPort: {}\n fsPort: {}\n host: {}", vmId, apiPort, fsPort, realHost);


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

        Objects.requireNonNull(allocatorToken);

        operationService = new LocalOperationService(vmId);
        this.envFactory = new EnvironmentFactory(defaultUserImage);

        server = newGrpcServer("0.0.0.0", apiPort, GrpcUtils.NO_AUTH)
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
            final var cm = HostAndPort.fromString(channelManagerAddress);
            final var iam = HostAndPort.fromString(iamAddress);

            lzyFsRoot = fsRoot;
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
            server.shutdown();
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        try {
            allocatorAgent = new AllocatorAgent(allocatorToken, vmId, allocatorAddress, allocatorHeartbeatPeriod);
            allocatorAgent.start(Map.of("PUBLIC_KEY", iamKeys.publicKey()));
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

            final var worker = new Worker(
                vmId, parse.getOptionValue("allocator-address"), parse.getOptionValue("iam"),
                allocHeartbeat == null ? null : Duration.parse(allocHeartbeat),
                Integer.parseInt(parse.getOptionValue('p')), Integer.parseInt(parse.getOptionValue('q')),
                parse.getOptionValue('m'), parse.getOptionValue("channel-manager"), parse.getOptionValue('h'),
                parse.getOptionValue("allocator-token"), parse.getOptionValue("user-default-image"));

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
    public static void setRsaKeysForTests(@Nullable RsaUtils.RsaKeys keys) {
        RSA_KEYS = keys;
    }

    @VisibleForTesting
    public static void useLocalhostAsHost(boolean use) {
        USE_LOCALHOST_AS_HOST = use;
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    private class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {

        private synchronized ExecuteResponse executeOp(ExecuteRequest request) {
            String tid = request.getTaskId();
            var task = TaskDesc.fromProto(request.getTaskDesc());

            var lzySlots = new ArrayList<LzySlot>(task.operation().slots().size());

            for (var slot : task.operation().slots()) {
                final var binding = task.slotsToChannelsAssignments().get(slot.name());
                if (binding == null) {

                    return ExecuteResponse.newBuilder()
                        .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                        .setDescription("Internal error")
                        .build();
                }

                lzySlots.add(lzyFs.getSlotsManager().getOrCreateSlot(tid, slot, binding));
            }

            lzySlots.forEach(slot -> {
                if (slot instanceof LzyFileSlot) {
                    lzyFs.addSlot((LzyFileSlot) slot);
                }
            });

            final var stdoutSpec = task.operation().stdout();
            final var stderrSpec = task.operation().stderr();

            final var outQueue = generateStreamQueue(tid, stdoutSpec, "stdout");
            final var errQueue = generateStreamQueue(tid, stderrSpec, "stderr");

            outQueue.start();
            errQueue.start();

            try {

                LOG.info("Configuring worker");

                final Environment env = envFactory.create(lzyFsRoot, ProtoConverter.fromProto(
                        request.getTaskDesc().getOperation().getEnv()));

                try {
                    env.install(outQueue, errQueue);
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

                LOG.info("Executing task");

                try {
                    var exec = new Execution(tid, task.operation().command(), "", lzyFs.getMountPoint().toString());

                    exec.start(env);

                    outQueue.add(exec.process().out());
                    errQueue.add(exec.process().err());

                    final int rc = exec.waitFor();
                    final String message;

                    if (rc == 0) {
                        message = "Success";
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
            } finally {
                try {
                    outQueue.close();
                    errQueue.close();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while closing out stream");
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
