package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class TerminalConnection {
    private static final Logger LOG = LogManager.getLogger(TerminalConnection.class);

    private final StreamObserver<Kharon.TerminalCommand> terminalController;
    private final Map<UUID, CompletableFuture<Kharon.TerminalState>> terminalCommands = new ConcurrentHashMap<>();
    private String user;

    public TerminalConnection(StreamObserver<Kharon.TerminalCommand> terminalController) {
        this.terminalController = terminalController;
    }

    public String user() {
        return user;
    }

    @Nullable
    public Servant.SlotCommandStatus configureSlot(Servant.SlotCommand request) {
        LOG.info("Kharon sessionId " + sessionId + " ::configureSlot " + JsonUtils.printRequest(request));
        final CompletableFuture<Kharon.TerminalState> future = new CompletableFuture<>();

        final UUID commandId = UUID.randomUUID();
        terminalCommands.put(commandId, future);

        final Kharon.TerminalCommand sendingRequest = Kharon.TerminalCommand.newBuilder()
                .setCommandId(commandId.toString())
                .setSlotCommand(request)
                .build();
        LOG.info("terminalController send request " + JsonUtils.printRequest(sendingRequest));
        synchronized (terminalController) {
            try {
                terminalController.onNext(sendingRequest);
            } catch (RuntimeException e) { // FIXME(d-kruchinin): why cannot catch StatusRuntimeException?
                LOG.warn(
                        "Kharon session={} was cancelled, but got configureSlot return -1;\n Cause: {}",
                        sessionId, e);
                return Servant.SlotCommandStatus.newBuilder()
                        .setRc(Servant.SlotCommandStatus.RC.newBuilder().setCodeValue(-1)).build();
            }
        }
        try {
            return future.get(10, TimeUnit.SECONDS).getSlotStatus();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed while configure slot in bidirectional stream " + e);
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    public void slotStatus(UUID commandId, Servant.SlotCommandStatus slotStatus) {
        final CompletableFuture<Kharon.TerminalState> future = terminalCommands.get(commandId);
        if (future == null) {
            throw new IllegalStateException("No such terminal command " + commandId);
        }
        future.complete(terminalState);
    }
}
