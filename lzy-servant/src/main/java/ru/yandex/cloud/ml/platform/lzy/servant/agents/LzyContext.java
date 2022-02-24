package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Context;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.ReturnCodes;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvironmentFactory;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFSManager;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.InFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LineReaderSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LocalOutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LzySlotBase;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.OutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.WriterSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetS3CredentialsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextConcluded;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextStarted;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ExecutionProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.PreparationError;

public class LzyContext {
    private static final Logger LOG = LogManager.getLogger(LzyContext.class);
    private final LineReaderSlot stdoutSlot;
    private final LineReaderSlot stderrSlot;
    private final WriterSlot stdinSlot;
    private final Map<String, LzySlot> slots = new ConcurrentHashMap<>();
    private final LockManager lockManager = new LocalLockManager();
    private final Snapshotter snapshotter;
    private final String contextId;
    private final URI servantUri;
    private final List<Consumer<Servant.ContextProgress>> listeners = new ArrayList<>();
    private final GetS3CredentialsResponse credentials;
    private String arguments = "";
    private Environment env;

    public LzyContext(
        String contextId, Snapshotter snapshotter, URI servantUri,
        GetS3CredentialsResponse credentials
    ) {
        this.contextId = contextId;
        stdinSlot = new WriterSlot(contextId, new TextLinesInSlot("/dev/stdin"), snapshotter.snapshotProvider());
        stdoutSlot = new LineReaderSlot(contextId, new TextLinesOutSlot("/dev/stdout"), snapshotter.snapshotProvider());
        stderrSlot = new LineReaderSlot(contextId, new TextLinesOutSlot("/dev/stderr"), snapshotter.snapshotProvider());
        this.snapshotter = snapshotter;
        this.servantUri = servantUri;
        this.credentials = credentials;
    }

    public Stream<LzySlot> slots() {
        return slots.values().stream();
    }

    public LzySlot slot(String name) {
        return slots.get(name);
    }

    public LzySlot configureSlot(Slot spec, String binding) {
        LOG.info("LzyExecution::configureSlot " + spec.name() + " binding: " + binding);
        final Lock lock = lockManager.getOrCreate(spec.name());
        lock.lock();
        try {
            if (slots.containsKey(spec.name())) {
                return slots.get(spec.name());
            }
            try {
                snapshotter.prepare(spec);
                final LzySlot slot = createSlot(spec, binding);
                if (slot.state() != Operations.SlotStatus.State.DESTROYED) {
                    LOG.info("LzyExecution::Slots.put(\n" + spec.name() + ",\n" + slot + "\n)");
                    if (spec.name().startsWith("local://")) { // No scheme in slot name
                        slots.put(spec.name().substring("local://".length()), slot);
                    } else {
                        slots.put(spec.name(), slot);
                    }
                }

                slot.onState(
                    Operations.SlotStatus.State.SUSPENDED,
                    () -> {
                        //not terminal or input slot
                        if (spec.direction() == Slot.Direction.INPUT) {
                            progress(Servant.ContextProgress.newBuilder()
                                .setDetach(Servant.SlotDetach.newBuilder()
                                    .setSlot(GrpcConverter.to(spec))
                                    .setUri(servantUri.toString() + spec.name())
                                    .build()
                                ).build()
                            );
                        }
                    }
                );
                slot.onState(Operations.SlotStatus.State.DESTROYED, () -> {
                    synchronized (slots) {
                        LOG.info("LzyContext::Slots.remove(\n" + slot.name() + "\n)");
                        snapshotter.commit(spec);
                        slots.remove(slot.name());
                        slots.notifyAll();
                    }
                });
                if (binding == null) {
                    binding = "";
                } else if (binding.startsWith("channel:")) {
                    binding = binding.substring("channel:".length());
                }

                final String slotPath = URI.create(spec.name()).getPath();
                progress(Servant.ContextProgress.newBuilder().setAttach(
                    Servant.SlotAttach.newBuilder()
                        .setChannel(binding)
                        .setSlot(GrpcConverter.to(spec))
                        .setUri(servantUri.toString() + slotPath)
                        .build()
                ).build());
                LOG.info("Configured slot " + spec.name() + " " + slot);
                return slot;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        } finally {
            lock.unlock();
        }
    }

    public void prepare(LzyFSManager fs, Context context) throws EnvironmentInstallationException {
        context.assignments().map(
            entry -> configureSlot(entry.slot(), entry.binding())
        ).forEach(slot -> {
            if (slot instanceof LzyFileSlot) {
                LOG.info("lzyFS::addSlot " + slot.name());
                fs.addSlot((LzyFileSlot) slot);
                LOG.info("lzyFS::slot added " + slot.name());
            }
        });

        try {
            env = EnvironmentFactory.create(context.env(), credentials);
        } catch (EnvironmentInstallationException e) {
            Set.copyOf(slots.values()).stream().filter(s -> s instanceof LzyInputSlot).forEach(LzySlot::suspend);
            Set.copyOf(slots.values()).stream()
                .filter(s -> s instanceof LzyOutputSlot)
                .map(s -> (LzyOutputSlot) s)
                .forEach(LzyOutputSlot::forceClose);
            progress(ContextProgress.newBuilder()
                .setError(PreparationError.newBuilder()
                    .setDescription("Error during environment installation:\n" + e)
                    .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                    .build())
                .build());
            throw e;
        }
        progress(ContextProgress.newBuilder()
            .setStart(ContextStarted.newBuilder().build())
            .build());
    }

    public LzyExecution execute(AtomicZygote zygote, Consumer<ExecutionProgress> onProgress)
        throws LzyExecutionException, InterruptedException {
        final long start = System.currentTimeMillis();
        if (env == null) {
            throw new LzyExecutionException(new RuntimeException("Cannot execute before prepare"));
        }

        LzyExecution execution = new LzyExecution(contextId, zygote, arguments);
        execution.onProgress(onProgress);
        execution.start(env);
        stdinSlot.setStream(new OutputStreamWriter(
            execution.lzyProcess().in(),
            StandardCharsets.UTF_8)
        );
        stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.lzyProcess().out(),
            StandardCharsets.UTF_8
        )));
        stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.lzyProcess().err(),
            StandardCharsets.UTF_8
        )));
        int rc = execution.waitFor();
        Set.copyOf(slots.values()).stream().filter(s -> s instanceof LzyInputSlot).forEach(LzySlot::suspend);
        if (rc != 0) {
            Set.copyOf(slots.values()).stream()
                .filter(s -> s instanceof LzyOutputSlot)
                .map(s -> (LzyOutputSlot) s)
                .forEach(LzyOutputSlot::forceClose);
        }
        final long executed = System.currentTimeMillis();
        MetricEventLogger.log(new MetricEvent(
            "time of task executing",
            Map.of("metric_type", "system_metric"),
            executed - start)
        );
        waitForSlots();
        MetricEventLogger.log(new MetricEvent(
            "time of waiting for slots",
            Map.of("metric_type", "system_metric"),
            System.currentTimeMillis() - executed)
        );
        return execution;
    }


    public LzySlot createSlot(Slot spec, String binding) throws IOException {
        final Lock lock = lockManager.getOrCreate(spec.name());
        lock.lock();
        try {
            if (spec.equals(Slot.STDIN)) {
                return stdinSlot;
            } else if (spec.equals(Slot.STDOUT)) {
                return stdoutSlot;
            } else if (spec.equals(Slot.STDERR)) {
                return stderrSlot;
            }

            switch (spec.media()) {
                case PIPE:
                case FILE: {
                    switch (spec.direction()) {
                        case INPUT:
                            return new InFileSlot(contextId, spec, snapshotter.snapshotProvider());
                        case OUTPUT:
                            if (spec.name().startsWith("local://")) {
                                return new LocalOutFileSlot(contextId, spec, URI.create(spec.name()),
                                    snapshotter.snapshotProvider());
                            }
                            return new OutFileSlot(contextId, spec, snapshotter.snapshotProvider());
                        default:
                            throw new UnsupportedOperationException("Not implemented yet");
                    }
                }
                case ARG:
                    arguments = binding;
                    return new LzySlotBase(spec, snapshotter.snapshotProvider()) {
                    };
                default:
                    throw new UnsupportedOperationException("Not implemented yet");
            }
        } finally {
            lock.unlock();
        }
    }

    public synchronized void progress(Servant.ContextProgress progress) {
        LOG.info("LzyContext::progress " + JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<Servant.ContextProgress> listener) {
        listeners.add(listener);
    }

    private void waitForSlots() throws InterruptedException {
        while (!slots.isEmpty()) {
            synchronized (slots) {
                LOG.info("Slots: " + Arrays.toString(slots().map(LzySlot::name).toArray()));
                slots.wait();
            }
        }
        progress(ContextProgress.newBuilder()
            .setExit(ContextConcluded.newBuilder().build())
            .build());
    }
}
