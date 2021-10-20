package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.local.TerminalEndpoint;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TerminalController {
    private static final Logger LOG = LogManager.getLogger(TerminalController.class);

    private final StreamObserver<Lzy.TerminalCommand> observer;
    private final Map<UUID, CompletableFuture<Lzy.TerminalState>> tasks = new ConcurrentHashMap<>();

    public TerminalController(StreamObserver<Lzy.TerminalCommand> observer) {
        this.observer = observer;
    }

    public void onSlotStatus(Lzy.TerminalState terminalState) {
        final UUID taskId = UUID.fromString(terminalState.getCommandId());
        final CompletableFuture<Lzy.TerminalState> future = tasks.get(taskId);
        if (future == null) {
            throw new IllegalStateException("No future for task with id " + taskId);
        }
        future.complete(terminalState);
    }

    private String generateCommandId(CompletableFuture<Lzy.TerminalState> future) {
        final UUID taskId = UUID.randomUUID();
        tasks.put(taskId, future);
        return taskId.toString();
    }

    public synchronized int connect(TerminalEndpoint terminalEndpoint, Endpoint endpoint) {
        final CompletableFuture<Lzy.TerminalState> future = new CompletableFuture<>();
        final Lzy.TerminalCommand terminalCommand = Lzy.TerminalCommand.newBuilder()
            .setCommandId(generateCommandId(future))
            .setSlotCommand(
                Servant.SlotCommand.newBuilder()
                    .setSlot(terminalEndpoint.slot().name())
                    .setConnect(Servant.ConnectSlotCommand.newBuilder()
                        .setSlotUri(endpoint.uri().toString()).build())
                    .build())
            .build();
        LOG.info("Terminal::connect from " + terminalEndpoint + " to " + endpoint +
            "\ncommand: " + JsonUtils.printRequest(terminalCommand));
        observer.onNext(terminalCommand);
        //try {
        //    return future.get().getSlotStatus().getRc().getCodeValue();
        //} catch (InterruptedException | ExecutionException e) {
        //    LOG.error("Exception via connect terminalEndpoint " + terminalEndpoint + " to " + endpoint);
        //}
        return 0;
    }

    @Nullable
    public synchronized SlotStatus status(TerminalEndpoint terminalEndpoint) {
        final CompletableFuture<Lzy.TerminalState> future = new CompletableFuture<>();
        observer.onNext(Lzy.TerminalCommand.newBuilder()
            .setCommandId(generateCommandId(future))
            .setSlotCommand(
                Servant.SlotCommand.newBuilder()
                    .setSlot(terminalEndpoint.slot().name())
                    .setStatus(Servant.StatusCommand.newBuilder().build())
                    .build())
            .build());
        try {
            return gRPCConverter.from(future.get(100, TimeUnit.SECONDS).getSlotStatus().getStatus());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Exception via slotStatus to terminalEndpoint " + terminalEndpoint);
        }
        return null;
    }

    public synchronized int disconnect(TerminalEndpoint terminalEndpoint) {
        final CompletableFuture<Lzy.TerminalState> future = new CompletableFuture<>();
        final Lzy.TerminalCommand terminalCommand = Lzy.TerminalCommand.newBuilder()
            .setCommandId(generateCommandId(future))
            .setSlotCommand(
                Servant.SlotCommand.newBuilder()
                    .setSlot(terminalEndpoint.slot().name())
                    .setDisconnect(Servant.DisconnectCommand.newBuilder().build())
                    .build()
            ).build();
        LOG.info("Terminal::disconnect " + terminalEndpoint +
            "\ncommand: " + JsonUtils.printRequest(terminalCommand));
        observer.onNext(terminalCommand);
        //try {
        //    return future.get().getSlotStatus().getRc().getCodeValue();
        //} catch (InterruptedException | ExecutionException e) {
        //    LOG.error("Exception via disconnect terminalEndpoint " + terminalEndpoint);
        //}
        return 0;
    }

    public synchronized int destroy(TerminalEndpoint terminalEndpoint) {
        final CompletableFuture<Lzy.TerminalState> future = new CompletableFuture<>();
        final Lzy.TerminalCommand terminalCommand = Lzy.TerminalCommand.newBuilder()
            .setCommandId(generateCommandId(future))
            .setSlotCommand(
                Servant.SlotCommand.newBuilder()
                    .setSlot(terminalEndpoint.slot().name())
                    .setDestroy(Servant.DestroyCommand.newBuilder().build())
                    .build()
            ).build();
        LOG.info("Terminal::destroy " + terminalEndpoint +
            "\ncommand: " + JsonUtils.printRequest(terminalCommand));
        observer.onNext(terminalCommand);
        //try {
        //    return future.get().getSlotStatus().getRc().getCodeValue();
        //} catch (InterruptedException | ExecutionException e) {
        //    LOG.error("Exception via destroying terminalEndpoint " + terminalEndpoint);
        //}
        return 0;
    }
}
