package ai.lzy.worker;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.logs.KafkaConfig.KafkaHelper;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.StreamQueue.LogHandle;
import ai.lzy.worker.env.AuxEnvironment;
import ai.lzy.worker.env.EnvironmentFactory;
import ai.lzy.worker.env.EnvironmentInstallationException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@Singleton
class WorkerApiImpl extends WorkerApiGrpc.WorkerApiImplBase {
    private static final Logger LOG = LogManager.getLogger(WorkerApiImpl.class);
    public static volatile boolean TEST_ENV = false;

    private final LzyFsServer lzyFs;
    private final LocalOperationService operationService;
    private final EnvironmentFactory envFactory;
    private final KafkaHelper kafkaHelper;

    public WorkerApiImpl(@Named("WorkerOperationService") LocalOperationService localOperationService,
                         EnvironmentFactory environmentFactory, LzyFsServer lzyFsServer,
                         @Named("WorkerKafkaHelper") KafkaHelper helper)
    {
        this.kafkaHelper = helper;
        this.operationService = localOperationService;
        this.envFactory = environmentFactory;
        this.lzyFs = lzyFsServer;
    }

    private synchronized LWS.ExecuteResponse executeOp(LWS.ExecuteRequest request) {
        String tid = request.getTaskId();
        var task = request.getTaskDesc();
        var op = task.getOperation();

        var lzySlots = new ArrayList<LzySlot>(task.getOperation().getSlotsCount());
        var slotAssignments = task.getSlotAssignmentsList().stream()
            .collect(Collectors.toMap(LMO.SlotToChannelAssignment::getSlotName,
                LMO.SlotToChannelAssignment::getChannelId));

        for (var slot : op.getSlotsList()) {
            final var binding = slotAssignments.get(slot.getName());
            if (binding == null) {
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

        var topicDesc = request.getTaskDesc().getOperation().hasKafkaTopicDesc()
            ? request.getTaskDesc().getOperation().getKafkaTopicDesc()
            : null;

        try (final var logHandle = LogHandle.fromTopicDesc(LOG, topicDesc, kafkaHelper)) {

            final var stdoutSpec = op.hasStdout() ? op.getStdout() : null;
            final var stderrSpec = op.hasStderr() ? op.getStderr() : null;
            logHandle.addErrOutput(generateStdOutputStream(tid, stderrSpec));
            logHandle.addOutOutput(generateStdOutputStream(tid, stdoutSpec));

            LOG.info("Configuring worker");

            final AuxEnvironment env = envFactory.create(lzyFs.getMountPoint().toString(),
                request.getTaskDesc().getOperation().getEnv());

            try {
                env.base().install(logHandle);
                env.install(logHandle);
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
                Thread.sleep(Duration.ofSeconds(1).toMillis());
            }
        }
    }

    private OutputStream generateStdOutputStream(String tid, @Nullable LMO.Operation.StdSlotDesc stdoutSpec) {
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
    public void execute(LWS.ExecuteRequest request, StreamObserver<LongRunning.Operation> response) {
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

        LocalOperationService.OperationSnapshot opSnapshot = operationService.execute(op, () -> executeOp(request));

        response.onNext(opSnapshot.toProto());
        response.onCompleted();
    }

    private boolean loadExistingOpResult(Operation.IdempotencyKey key,
                                         StreamObserver<LongRunning.Operation> response)
    {
        return IdempotencyUtils.loadExistingOp(operationService, key, response, LOG);
    }
}
