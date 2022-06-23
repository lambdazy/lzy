package ai.lzy.kharon;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsApi;

public class TerminalController {
    private static final Logger LOG = LogManager.getLogger(TerminalController.class);

    private final StreamObserver<Kharon.TerminalCommand> commandStreamObserver;
    private final Map<UUID, CompletableFuture<LzyFsApi.SlotCommandStatus>> terminalCommands = new ConcurrentHashMap<>();
    private final AtomicBoolean invalidated = new AtomicBoolean(false);

    public TerminalController(StreamObserver<Kharon.TerminalCommand> commandStreamObserver) {
        this.commandStreamObserver = commandStreamObserver;
    }

    public LzyFsApi.SlotCommandStatus configureSlot(LzyFsApi.SlotCommand request)
        throws TerminalControllerResetException {
        final CompletableFuture<LzyFsApi.SlotCommandStatus> future = new CompletableFuture<>();

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
                throw new TerminalControllerResetException(e);
            }
        }
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed while configure slot in bidirectional stream " + e);
            terminate(Status.DEADLINE_EXCEEDED.withCause(e).asRuntimeException());
        }
        return LzyFsApi.SlotCommandStatus.newBuilder().build();
    }

    public void handleTerminalResponse(Kharon.TerminalResponse response) {
        final UUID commandId = UUID.fromString(response.getCommandId());
        final LzyFsApi.SlotCommandStatus slotStatus = response.getSlotStatus();
        final CompletableFuture<LzyFsApi.SlotCommandStatus> future = terminalCommands.get(commandId);
        if (future == null) {
            throw new IllegalStateException("No such terminal command " + commandId);
        }
        future.complete(slotStatus);
    }

    public void terminate(Throwable th) {
        if (invalidated.compareAndSet(false, true)) {
            synchronized (commandStreamObserver) {
                commandStreamObserver.onError(th);
            }
        }
    }

    public void complete() {
        if (invalidated.compareAndSet(false, true)) {
            synchronized (commandStreamObserver) {
                commandStreamObserver.onCompleted();
            }
        }
    }

    public void onDisconnect() {
        invalidated.compareAndSet(false, true);
    }

    public static class TerminalControllerResetException extends Exception {
        public TerminalControllerResetException(Throwable th) {
            super(th);
        }
    }
}
