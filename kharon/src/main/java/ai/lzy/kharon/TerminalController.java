package ai.lzy.kharon;

import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.deprecated.Kharon;
import ai.lzy.v1.fs.LzyFsApi;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class TerminalController {
    private static final Logger LOG = LogManager.getLogger(TerminalController.class);

    private final StreamObserver<Kharon.TerminalCommand> commandStreamObserver;
    private final Map<UUID, CompletableFuture<LzyFsApi.SlotCommandStatus>> terminalCommands = new ConcurrentHashMap<>();
    private final AtomicBoolean invalidated = new AtomicBoolean(false);

    public TerminalController(StreamObserver<Kharon.TerminalCommand> commandStreamObserver) {
        this.commandStreamObserver = commandStreamObserver;
    }

    private <R> LzyFsApi.SlotCommandStatus call(R request,
            BiFunction<R, Kharon.TerminalCommand.Builder, Kharon.TerminalCommand.Builder> fn)
            throws TerminalControllerResetException {
        final CompletableFuture<LzyFsApi.SlotCommandStatus> future = new CompletableFuture<>();

        final UUID commandId = UUID.randomUUID();
        terminalCommands.put(commandId, future);

        var sendingRequestBuilder = Kharon.TerminalCommand.newBuilder()
                .setCommandId(commandId.toString());
        var sendingRequest = fn.apply(request, sendingRequestBuilder).build();

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
            LOG.error("Failed while configure slot in bidirectional stream commandId={}", commandId, e);
            terminate(Status.DEADLINE_EXCEEDED.withCause(e).asRuntimeException());
        }
        return LzyFsApi.SlotCommandStatus.newBuilder().build();
    }

    public LzyFsApi.SlotCommandStatus createSlot(LzyFsApi.CreateSlotRequest request)
            throws TerminalControllerResetException {
        return call(request, (req, builder) -> builder.setCreateSlot(req));
    }

    public LzyFsApi.SlotCommandStatus connectSlot(LzyFsApi.ConnectSlotRequest request)
            throws TerminalControllerResetException {
        return call(request, (req, builder) -> builder.setConnectSlot(req));
    }

    public LzyFsApi.SlotCommandStatus disconnectSlot(LzyFsApi.DisconnectSlotRequest request)
            throws TerminalControllerResetException {
        return call(request, (req, builder) -> builder.setDisconnectSlot(req));
    }

    public LzyFsApi.SlotCommandStatus statusSlot(LzyFsApi.StatusSlotRequest request)
            throws TerminalControllerResetException {
        return call(request, (req, builder) -> builder.setStatusSlot(req));
    }

    public LzyFsApi.SlotCommandStatus destroySlot(LzyFsApi.DestroySlotRequest request)
            throws TerminalControllerResetException {
        return call(request, (req, builder) -> builder.setDestroySlot(req));
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

    public static class TerminalControllerResetException extends Exception {
        public TerminalControllerResetException(Throwable th) {
            super(th);
        }
    }
}
