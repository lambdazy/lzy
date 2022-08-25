package ai.lzy.fs;

import ai.lzy.model.SlotInstance;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.LzyChannelManagerGrpc;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.ArgumentsSlot;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.slots.TextLinesOutSlot;
import ai.lzy.v1.Servant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static ai.lzy.model.GrpcConverter.to;
import static ai.lzy.v1.Operations.SlotStatus.State.DESTROYED;
import static ai.lzy.v1.Operations.SlotStatus.State.SUSPENDED;

// TODO(artolord) remove progress from here in v2 servant
public class SlotsManager implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SlotsManager.class);

    private final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager;
    private final URI localLzyFsUri;
    // TODO: project?
    // { task -> { slot -> LzySlot } }
    private final Map<String, Map<String, LzySlot>> task2slots = new ConcurrentHashMap<>();
    private final List<Consumer<Servant.ServantProgress>> listeners = new ArrayList<>();
    private boolean closed = false;

    public SlotsManager(LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager, URI localLzyFsUri) {
        this.channelManager = channelManager;
        this.localLzyFsUri = localLzyFsUri;
    }

    @Override
    public synchronized void close() throws InterruptedException {
        if (closed) {
            return;
        }
        LOG.info("Close SlotsManager...");
        int iter = 100;
        while (!task2slots.isEmpty() && iter-- > 0) {
            LOG.info("Waiting for slots: {}...", Arrays.toString(slots().map(LzySlot::name).toArray()));
            this.wait(1_000);
        }
        // TODO (tomato): it is better to move progress updates from SlotsManager
        progress(Servant.ServantProgress.newBuilder().setConcluded(Servant.Concluded.newBuilder().build()).build());
        closed = true;
    }

    @Nullable
    public LzySlot slot(String task, String slot) {
        var taskSlots = task2slots.get(task);
        return taskSlots != null ? taskSlots.get(slot) : null;
    }

    public Stream<LzySlot> slots() {
        return Set.copyOf(task2slots.values()).stream()
            .flatMap(slots -> Set.copyOf(slots.values()).stream());
    }

    public synchronized void onProgress(Consumer<Servant.ServantProgress> listener) {
        listeners.add(listener);
    }

    public synchronized void reportProgress(Servant.ServantProgress progress) {
        progress(progress);
    }

    public URI resolveSlotUri(String taskId, String slotName) {
        return localLzyFsUri.resolve(Path.of("/", taskId, slotName).toString());
    }

    public synchronized LzySlot getOrCreateSlot(String taskId, Slot spec, final String channelId) {
        LOG.info("getOrCreateSlot, taskId: {}, spec: {}, binding: {}", taskId, spec.name(), channelId);

        final Map<String, LzySlot> taskSlots = task2slots.computeIfAbsent(taskId, t -> new HashMap<>());
        final LzySlot existing = taskSlots.get(spec.name());
        if (existing != null) {
            return existing;
        }

        try {
            final LzySlot slot = createSlot(taskId, spec, channelId);

            if (slot.state() == DESTROYED) {
                final String msg = MessageFormat.format("Unable to create slot. Task: {}, spec: {}, binding: {}",
                        taskId, spec.name(), channelId);
                LOG.error(msg);
                throw new RuntimeException(msg);
            }

            registerSlot(slot);
            return slot;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized void registerSlot(LzySlot slot) {
        final String taskId = slot.taskId();
        final Map<String, LzySlot> taskSlots = task2slots.computeIfAbsent(taskId, t -> new HashMap<>());
        if (taskSlots.containsKey(slot.name())) {
            throw new RuntimeException("Slot is already registered");
        }

        final var spec = slot.definition();
        final var slotUri = slot.instance().uri();
        final var channelId = slot.instance().channelId();

        if (slot.state() != DESTROYED) {
            taskSlots.put(spec.name(), slot);
        } else {
            var msg = MessageFormat.format("Unable to create slot. Task: {}, spec: {}", taskId, spec.name());
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        slot.onState(SUSPENDED, () -> {
            synchronized (SlotsManager.this) {
                LOG.info("UnBind slot {} from channel {}", spec, channelId);
                try {
                    final ChannelManager.SlotDetachStatus unbindResult = channelManager.unbind(
                        ChannelManager.SlotDetach.newBuilder()
                            .setSlotInstance(to(slot.instance()))
                            .build()
                    );
                    LOG.info(JsonUtils.printRequest(unbindResult));
                } catch (StatusRuntimeException e) {
                    LOG.warn("Got exception while unbind slot {} from channel {}", spec.name(), channelId, e);
                } finally {
                    SlotsManager.this.notifyAll();
                }
            }
        });

        slot.onState(DESTROYED, () -> {
            synchronized (SlotsManager.this) {
                taskSlots.remove(slot.name());
                LOG.info("SlotsManager:: Slot {} was removed", slot.name());
                if (taskSlots.isEmpty()) {
                    task2slots.remove(taskId);
                    progress(Servant.ServantProgress.newBuilder()
                        .setCommunicationCompleted(Servant.CommunicationCompleted.newBuilder().build())
                        .build());
                }
                SlotsManager.this.notifyAll();
            }
        });

        final ChannelManager.SlotAttachStatus slotAttachStatus = channelManager.bind(
            ChannelManager.SlotAttach.newBuilder()
                .setSlotInstance(to(slot.instance()))
                .build());
        LOG.info(JsonUtils.printRequest(slotAttachStatus));
        LOG.info("Slot `{}` configured.", slotUri);
    }

    private LzySlot createSlot(String taskId, Slot spec, String channelId) throws IOException {
        final URI slotUri = resolveSlotUri(taskId, spec.name());
        final SlotInstance slotInstance = new SlotInstance(spec, taskId, channelId, slotUri);
        if (Slot.STDIN.equals(spec)) {
            throw new AssertionError();
        }

        if (spec instanceof TextLinesOutSlot) {
            return new LineReaderSlot(new SlotInstance(spec, taskId, channelId, slotUri));
        }

        return switch (spec.media()) {
            case PIPE, FILE -> switch (spec.direction()) {
                case INPUT -> new InFileSlot(slotInstance);
                case OUTPUT -> new OutFileSlot(slotInstance);
            };
            case ARG -> new ArgumentsSlot(slotInstance, channelId);
        };
    }

    // call with global sync only
    private void progress(Servant.ServantProgress progress) {
        LOG.info("Progress {}", JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }
}
