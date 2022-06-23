package ai.lzy.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.ArgumentsSlot;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.fs.slots.LocalOutFileSlot;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.fs.slots.WriterSlot;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.slots.TextLinesInSlot;
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
        while (!task2slots.isEmpty()) {
            LOG.info("Waiting for slots: {}...", Arrays.toString(slots().map(LzySlot::name).toArray()));
            this.wait();
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

    public synchronized LzySlot configureSlot(String task, Slot spec, @Nullable String binding) {
        LOG.info("Configure slot, task: {}, spec: {}, binding: {}", task, spec.name(), binding);

        final Map<String, LzySlot> taskSlots = task2slots.computeIfAbsent(task, t -> new HashMap<>());
        final LzySlot existing = taskSlots.get(spec.name());
        if (existing != null) {
            return existing;
        }

        final URI slotUri = localLzyFsUri.resolve(Path.of("/", task, spec.name()).toString());

        try {
            final LzySlot slot = createSlot(spec, binding);
            if (slot.state() != DESTROYED) {
                if (spec.name().startsWith("local://")) { // No scheme in slot name
                    taskSlots.put(spec.name().substring("local://".length()), slot);
                } else {
                    taskSlots.put(spec.name(), slot);
                }
            } else {
                final String msg = MessageFormat.format("Unable to create slot. Task: {}, spec: {}, binding: {}",
                    task, spec.name(), binding);
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
                        task2slots.remove(task);
                        progress(Servant.ServantProgress.newBuilder()
                            .setCommunicationCompleted(Servant.CommunicationCompleted.newBuilder().build())
                            .build());
                    }
                    SlotsManager.this.notifyAll();
                }
            });

            if (binding != null && binding.startsWith("channel:")) {
                binding = binding.substring("channel:".length());
            }

            final Servant.SlotAttach.Builder attachBuilder = Servant.SlotAttach.newBuilder()
                .setSlot(GrpcConverter.to(spec))
                .setUri(slotUri.toString());
            if (binding != null) {
                attachBuilder.setChannel(binding);
            }

            progress(Servant.ServantProgress.newBuilder()
                .setAttach(attachBuilder.build())
                .build());

            LOG.info("Slot `{}` configured.", slotUri);
            return slot;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private LzySlot createSlot(Slot spec, @Nullable String binding) throws IOException {
        if (spec.equals(Slot.STDIN)) {
            return new WriterSlot(contextId, new TextLinesInSlot(spec.name()));
        }

        if (spec.equals(Slot.STDOUT)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        }

        if (spec.equals(Slot.STDERR)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        }

        switch (spec.media()) {
            case PIPE, FILE -> {
                switch (spec.direction()) {
                    case INPUT:
                        return new InFileSlot(contextId, spec);
                    case OUTPUT:
                        if (spec.name().startsWith("local://")) {
                            return new LocalOutFileSlot(contextId, spec, URI.create(spec.name()));
                        }
                        return new OutFileSlot(contextId, spec);
                    default:
                        throw new IllegalStateException("Unexpected value: " + spec.direction());
                }
            }
            case ARG -> {
                return new ArgumentsSlot(spec, binding);
            }
            default -> throw new IllegalStateException("Unexpected value: " + spec.media());
        }
    }

    // call with global sync only
    private void progress(Servant.ServantProgress progress) {
        LOG.info("Progress {}", JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }
}
