package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class TerminalController {
    private static final Logger LOG = LogManager.getLogger(TerminalController.class);

    private final StreamObserver<Kharon.TerminalCommand> commandStreamObserver;
    private final Map<UUID, CompletableFuture<Servant.SlotCommandStatus>> terminalCommands = new ConcurrentHashMap<>();

    public TerminalController(StreamObserver<Kharon.TerminalCommand> commandStreamObserver) {
        this.commandStreamObserver = commandStreamObserver;
    }

    public Servant.SlotCommandStatus configureSlot(Servant.SlotCommand request) {
        LOG.info("Kharon sessionId " + sessionId + " configureSlot " + JsonUtils.printRequest(request));
        final CompletableFuture<Servant.SlotCommandStatus> future = new CompletableFuture<>();

        final UUID commandId = UUID.randomUUID();
        terminalCommands.put(commandId, future);

        final Kharon.TerminalCommand sendingRequest = Kharon.TerminalCommand.newBuilder()
                .setCommandId(commandId.toString())
                .setSlotCommand(request)
                .build();
        LOG.info("terminalController send request " + JsonUtils.printRequest(sendingRequest));
        synchronized (commandStreamObserver) {
            try {
                commandStreamObserver.onNext(sendingRequest);
            } catch (RuntimeException e) { // FIXME(d-kruchinin): why cannot catch StatusRuntimeException?
                LOG.warn(
                    "Kharon terminal stream for session={} was cancelled, "
                    + "but got configureSlot, returning -1;\n Cause: {}",
                    sessionId, e);
                return Servant.SlotCommandStatus.newBuilder()
                        .setRc(Servant.SlotCommandStatus.RC.newBuilder().setCodeValue(-1)).build();
            }
        }
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed while configure slot in bidirectional stream " + e);
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    public void handleTerminalResponse(Kharon.TerminalResponse response) {
        final UUID commandId = UUID.fromString(response.getCommandId());
        final Servant.SlotCommandStatus slotStatus = response.getSlotStatus();
        final CompletableFuture<Servant.SlotCommandStatus> future = terminalCommands.get(commandId);
        if (future == null) {
            throw new IllegalStateException("No such terminal command " + commandId);
        }
        future.complete(slotStatus);
    }
}
