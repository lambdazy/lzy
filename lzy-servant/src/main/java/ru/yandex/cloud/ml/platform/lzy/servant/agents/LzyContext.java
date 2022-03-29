package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.*;
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
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.*;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetS3CredentialsResponse;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.InFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LineReaderSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LocalOutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LzySlotBase;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.OutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.WriterSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
    private final List<Consumer<Servant.ContextProgress>> listeners = new ArrayList<>();
    private final Map<String, Map<String, LzySlot>> namespaces = new HashMap<>();
    private String arguments = "";
    private Environment env;

    public LzyContext(String contextId, SlotConnectionManager snapshooter,
                      URI servantUri, GetS3CredentialsResponse credentials) {
        this.contextId = contextId;
        this.slotsManager = snapshooter;
        this.servantUri = servantUri;
        this.storage = StorageClient.create(credentials);
    }

    public synchronized Stream<LzySlot> slots() {
        return Set.copyOf(namespaces.values()).stream()
            .flatMap(stringLzySlotMap -> Set.copyOf(stringLzySlotMap.values()).stream());
    }

    public synchronized LzySlot slot(String task, String name) {
        return namespaces.getOrDefault(task, Map.of()).get(name);
    }

    public synchronized LzySlot configureSlot(String task, Slot spec, String binding) {
        LOG.info("LzyExecution::configureSlot " + spec.name() + " binding: " + binding);
        final Map<String, LzySlot> slots = namespaces.computeIfAbsent(task, t -> new HashMap<>());
        if (slots.containsKey(spec.name())) {
            return slots.get(spec.name());
        }
        try {
            final LzySlot slot = createSlot(spec, binding);
            if (slot.state() != DESTROYED) {
                LOG.info("LzyExecution::Slots.put(\n" + spec.name() + ",\n" + slot + "\n)");
                if (spec.name().startsWith("local://")) { // No scheme in slot name
                    slots.put(spec.name().substring("local://".length()), slot);
                } else {
                    slots.put(spec.name(), slot);
                }
            }

            slot.onState(SUSPENDED,
                () -> progress(ContextProgress.newBuilder()
                    .setDetach(Servant.SlotDetach.newBuilder()
                        .setSlot(GrpcConverter.to(spec))
                        .setUri(servantUri.toString() + task + "/" + spec.name())
                        .build()
                    ).build()
                )
            );
            slot.onState(DESTROYED, () -> {
                synchronized (LzyContext.this) {
                    LOG.info("LzyContext::Slots.remove(\n" + slot.name() + "\n)");
                    slots.remove(slot.name());
                    if (slots.isEmpty())
                        namespaces.remove(task);
                    LzyContext.this.notifyAll();
                }
            });
            if (binding == null) {
                binding = "";
            } else if (binding.startsWith("channel:")) {
                binding = binding.substring("channel:".length());
            }

            final URI slotUri = servantUri.resolve("/" + task
                + (spec.name().startsWith("/") ? spec.name() : "/" + spec.name())
            );
            progress(Servant.ContextProgress.newBuilder().setAttach(
                Servant.SlotAttach.newBuilder()
                    .setChannel(binding)
                    .setSlot(GrpcConverter.to(spec))
                    .setUri(slotUri.toString())
                    .build()
            ).build());
            LOG.info("Configured slot " + slotUri + " " + slot);
            return slot;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized void prepare(LzyFSManager fs, Context context) throws EnvironmentInstallationException {
        context.assignments().map(
            entry -> configureSlot(entry.task(), entry.slot(), entry.binding())
        ).forEach(slot -> {
            if (slot instanceof LzyFileSlot) {
                LOG.info("lzyFS::addSlot " + slot.name());
                fs.addSlot((LzyFileSlot) slot);
                LOG.info("lzyFS::slot added " + slot.name());
            }
        });

        try {
            env = EnvironmentFactory.create(context.env(), storage);
        } catch (EnvironmentInstallationException e) {
            slots().forEach(LzySlot::close);
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

    public synchronized LzyExecution execute(String taskId, AtomicZygote zygote, Consumer<ExecutionProgress> onProgress)
        throws LzyExecutionException, InterruptedException {
        final long start = System.currentTimeMillis();
        if (env == null) {
            throw new LzyExecutionException(new RuntimeException("Cannot execute before prepare"));
        }

        LzyExecution execution = new LzyExecution(contextId, zygote, arguments);
        execution.onProgress(onProgress);
        execution.start(env);

        final WriterSlot stdinSlot = (WriterSlot) configureSlot(taskId, Slot.STDIN, "/dev/stdin");
        final LineReaderSlot stdoutSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDOUT, "/dev/stdout");
        final LineReaderSlot stderrSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDOUT, "/dev/stderr");

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
            return new WriterSlot(contextId, new TextLinesInSlot(binding));
        } else if (spec.equals(Slot.STDOUT)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot(binding));
        } else if (spec.equals(Slot.STDERR)) {
            return new LineReaderSlot(contextId, new TextLinesOutSlot("/dev/stderr"));
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

    private void progress(Servant.ContextProgress progress) {
        LOG.info("LzyContext::progress " + JsonUtils.printRequest(progress));
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<Servant.ContextProgress> listener) {
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
        progress(ContextProgress.newBuilder()
            .setExit(ContextConcluded.newBuilder().build())
            .build());
    }

    public SlotConnectionManager slotManager() {
        return slotsManager;
    }
}
