package ai.lzy.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.ArgumentsSlot;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.fs.slots.LocalOutFileSlot;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.slots.TextLinesOutSlot;
import ai.lzy.priv.v2.Servant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static ai.lzy.priv.v2.Operations.SlotStatus.State.DESTROYED;
import static ai.lzy.priv.v2.Operations.SlotStatus.State.SUSPENDED;

public class SlotsManager implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SlotsManager.class);

    private final URI localLzyFsUri;
    // TODO: project?
    private final String contextId;
    // { task -> { slot -> LzySlot } }
    private final Map<String, Map<String, LzySlot>> task2slots = new ConcurrentHashMap<>();
    private final List<Consumer<Servant.ServantProgress>> listeners = new ArrayList<>();
    private boolean closed = false;


    public SlotsManager(String contextId, URI localLzyFsUri) {
        this.contextId = contextId;
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

    public synchronized LzySlot getOrCreateSlot(String taskId, Slot spec, @Nullable String binding) {
        LOG.info("getOrCreateSlot, taskId: {}, spec: {}, binding: {}", taskId, spec.name(), binding);

        final Map<String, LzySlot> taskSlots = task2slots.computeIfAbsent(taskId, t -> new HashMap<>());
        final LzySlot existing = taskSlots.get(spec.name());
        if (existing != null) {
            return existing;
        }

        try {
            final LzySlot slot = createSlot(spec, binding);

            if (slot.state() == DESTROYED) {
                final String msg = MessageFormat.format("Unable to create slot. Task: {}, spec: {}, binding: {}",
                        taskId, spec.name(), binding);
                LOG.error(msg);
                throw new RuntimeException(msg);
            }

            if (binding != null && binding.startsWith("channel:")) {
                binding = binding.substring("channel:".length());
            }

            return registerSlot(taskId, slot, binding);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized LzySlot registerSlot(String taskId, LzySlot slot, @Nullable String channelId) {
        final Map<String, LzySlot> taskSlots = task2slots.computeIfAbsent(taskId, t -> new HashMap<>());
        if (taskSlots.containsKey(slot.name())) {
            throw new RuntimeException("Slot already exists");
        }

        var spec = slot.definition();
        var slotUri = resolveSlotUri(taskId, spec.name());

        if (slot.state() != DESTROYED) {
            if (spec.name().startsWith("local://")) { // No scheme in slot name
                taskSlots.put(spec.name().substring("local://".length()), slot);
            } else {
                taskSlots.put(spec.name(), slot);
            }
        } else {
            var msg = MessageFormat.format("Unable to create slot. Task: {}, spec: {}", taskId, spec.name());
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        slot.onState(SUSPENDED, () -> {
            synchronized (SlotsManager.this) {
                progress(Servant.ServantProgress.newBuilder()
                    .setDetach(Servant.SlotDetach.newBuilder()
                        .setSlot(GrpcConverter.to(spec))
                        .setUri(slotUri.toString())
                        .build())
                    .build());
                SlotsManager.this.notifyAll();
            }
        });

        slot.onState(DESTROYED, () -> {
            synchronized (SlotsManager.this) {
                taskSlots.remove(slot.name());
                if (taskSlots.isEmpty()) {
                    task2slots.remove(taskId);
                    progress(Servant.ServantProgress.newBuilder()
                        .setCommunicationCompleted(Servant.CommunicationCompleted.newBuilder().build())
                        .build());
                }
                SlotsManager.this.notifyAll();
            }
        });

        final Servant.SlotAttach.Builder attachBuilder = Servant.SlotAttach.newBuilder()
                .setSlot(GrpcConverter.to(spec))
                .setUri(slotUri.toString());

        if (channelId != null) {
            attachBuilder.setChannel(channelId);
        }

        progress(Servant.ServantProgress.newBuilder()
                .setAttach(attachBuilder.build())
                .build());

        LOG.info("Slot `{}` configured.", slotUri);
        return slot;
    }

    private LzySlot createSlot(Slot spec, @Nullable String binding) throws IOException {
        if (spec.equals(Slot.STDIN)) {
            throw new AssertionError();
        }

        if (spec.equals(Slot.STDOUT)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        }

        if (spec.equals(Slot.STDERR)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        }

        return switch (spec.media()) {
            case PIPE, FILE -> switch (spec.direction()) {
                case INPUT -> new InFileSlot(contextId, spec);
                case OUTPUT -> (spec.name().startsWith("local://")
                        ? new LocalOutFileSlot(contextId, spec, URI.create(spec.name()))
                        : new OutFileSlot(contextId, spec));
            };
            case ARG -> new ArgumentsSlot(spec, binding);
        };
    }

    // call with global sync only
    private void progress(Servant.ServantProgress progress) {
        LOG.info("Progress {}", JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }
}
