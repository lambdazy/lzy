package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvironmentFactory;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.*;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.Concluded;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ServantProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotAttach;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.DESTROYED;
import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.SUSPENDED;

/**
 * This class is a monitor since all operations with the context must be sequential
 */
public class LzyContext implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(LzyContext.class);
    private final SlotConnectionManager slotsManager;
    private final String contextId;
    private final StorageClient storage;
    private final URI servantUri;
    private final List<Consumer<ServantProgress>> listeners = new ArrayList<>();
    private final Map<String, Map<String, LzySlot>> namespaces = Collections.synchronizedMap(new HashMap<>());
    private String arguments = "";
    private Environment env;

    public LzyContext(String contextId, SlotConnectionManager snapshooter, URI servantUri) {
        this.contextId = contextId;
        this.slotsManager = snapshooter;
        this.servantUri = servantUri;
        this.storage = snapshooter.snapshooter().storage();
    }

    public void start() {
        progress(ServantProgress.newBuilder().setStart(Servant.Started.newBuilder().build()).build());
    }

    public Stream<LzySlot> slots() {
        return Set.copyOf(namespaces.values()).stream()
            .flatMap(stringLzySlotMap -> Set.copyOf(stringLzySlotMap.values()).stream());
    }

    public LzySlot slot(String task, String name) {
        return namespaces.getOrDefault(task, Map.of()).get(name);
    }

    public synchronized LzySlot configureSlot(String task, Slot spec, String binding) {
        final URI slotUri = servantUri.resolve("/" + task
            + (spec.name().startsWith("/") ? spec.name() : "/" + spec.name())
        );

        LOG.info("LzyExecution::configureSlot servant: " + servantUri + " task: " + task + "" + spec.name()
            + " binding: " + binding + " uri: " + slotUri);

        final Map<String, LzySlot> slots = namespaces.computeIfAbsent(task, t -> new HashMap<>());
        if (slots.containsKey(spec.name())) {
            return slots.get(spec.name());
        }
        try {
            final LzySlot slot = createSlot(spec, binding);
            if (slot.state() != DESTROYED) {
                if (spec.name().startsWith("local://")) { // No scheme in slot name
                    slots.put(spec.name().substring("local://".length()), slot);
                } else {
                    slots.put(spec.name(), slot);
                }
            } else {
                LOG.warn("Unable to create slot " + spec.name());
            }

            slot.onState(SUSPENDED,
                () -> {
                    progress(ServantProgress.newBuilder()
                        .setDetach(Servant.SlotDetach.newBuilder()
                            .setSlot(GrpcConverter.to(spec))
                            .setUri(slotUri.toString())
                            .build()
                        ).build()
                    );
                }
            );
            slot.onState(DESTROYED, () -> {
                synchronized (LzyContext.this) {
                    slots.remove(slot.name());
                    if (slots.isEmpty())
                        namespaces.remove(task);
                    LzyContext.this.notifyAll();
                }
            });
            if (binding != null && binding.startsWith("channel:")) {
                binding = binding.substring("channel:".length());
            }

            final SlotAttach.Builder attachBuilder = SlotAttach.newBuilder()
                .setSlot(GrpcConverter.to(spec))
                .setUri(slotUri.toString());
            if (binding != null)
                attachBuilder.setChannel(binding);
            progress(ServantProgress.newBuilder().setAttach(attachBuilder.build()).build());
            LOG.info("Configured slot " + slotUri);
            return slot;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized void prepare(Env from) throws EnvironmentInstallationException {
        env = EnvironmentFactory.create(from, storage);
    }

    public LzyExecution execute(String taskId, AtomicZygote zygote, Consumer<ServantProgress> onProgress)
        throws LzyExecutionException, InterruptedException {
        final long start = System.currentTimeMillis();
        if (env == null) {
            throw new LzyExecutionException(new RuntimeException("Cannot execute before prepare"));
        }

        LzyExecution execution = new LzyExecution(contextId, zygote, arguments);
        final WriterSlot stdinSlot = (WriterSlot) configureSlot(taskId, Slot.STDIN, null);
        final LineReaderSlot stdoutSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDOUT, null);
        final LineReaderSlot stderrSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDOUT, null);
        execution.onProgress(onProgress);
        execution.start(env);
        stdinSlot.setStream(new OutputStreamWriter(execution.process().in(), StandardCharsets.UTF_8));
        stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.process().out(),
            StandardCharsets.UTF_8
        )));
        stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.process().err(),
            StandardCharsets.UTF_8
        )));
        execution.waitFor();
        stdinSlot.destroy();

        final long executed = System.currentTimeMillis();
        MetricEventLogger.log(new MetricEvent(
            "time of task executing",
            Map.of("metric_type", "system_metric"),
            executed - start)
        );
        MetricEventLogger.log(new MetricEvent(
            "time of waiting for slots",
            Map.of("metric_type", "system_metric"),
            System.currentTimeMillis() - executed)
        );
        return execution;
    }


    private LzySlot createSlot(Slot spec, String binding) throws IOException {
        if (spec.equals(Slot.STDIN)) {
            return new WriterSlot(contextId, new TextLinesInSlot(spec.name()));
        } else if (spec.equals(Slot.STDOUT)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        } else if (spec.equals(Slot.STDERR)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(spec.name()));
        }

        switch (spec.media()) {
            case PIPE:
            case FILE: {
                switch (spec.direction()) {
                    case INPUT:
                        return new InFileSlot(contextId, spec);
                    case OUTPUT:
                        if (spec.name().startsWith("local://")) {
                            return new LocalOutFileSlot(contextId, spec, URI.create(spec.name()));
                        }
                        return new OutFileSlot(contextId, spec);
                    default:
                        throw new UnsupportedOperationException("Not implemented yet");
                }
            }
            case ARG:
                arguments = binding;
                return new LzySlotBase(spec) {
                };
            default:
                throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    private void progress(ServantProgress progress) {
        LOG.info("LzyContext::progress " + JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<ServantProgress> listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void close() throws Exception {
        while (!namespaces.isEmpty()) {
            LOG.info("Slots: " + Arrays.toString(slots().map(LzySlot::name).toArray()));
            namespaces.wait();
        }
        if (slotsManager.snapshooter() != null) {
            slotsManager.snapshooter().close();
        }
        progress(ServantProgress.newBuilder()
            .setExit(Concluded.newBuilder().build())
            .build());
    }

    public SlotConnectionManager slotManager() {
        return slotsManager;
    }
}
