package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Context;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.ReturnCodes;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.env.BaseEnvConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvironmentFactory;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
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
    private final String taskId;
    private String arguments = "";
    private final URI servantUri;
    private final List<Consumer<Servant.ContextProgress>> listeners = new ArrayList<>();
    private final GetS3CredentialsResponse credentials;

    public LzyContext(String taskId, Snapshotter snapshotter, URI servantUri, GetS3CredentialsResponse credentials){
        this.taskId = taskId;
        stdinSlot = new WriterSlot(taskId, new TextLinesInSlot("/dev/stdin"), snapshotter.snapshotProvider());
        stdoutSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stdout"), snapshotter.snapshotProvider());
        stderrSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stderr"), snapshotter.snapshotProvider());
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
                                    .setSlot(gRPCConverter.to(spec))
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
                        .setSlot(gRPCConverter.to(spec))
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

    public Environment prepare(LzyFSManager fs, Context context) throws EnvironmentInstallationException {
        context.assignments().stream().map(
            entry -> configureSlot(entry.slot(), entry.binding())
        ).forEach(slot -> {
            if (slot instanceof LzyFileSlot) {
                LOG.info("lzyFS::addSlot " + slot.name());
                fs.addSlot((LzyFileSlot) slot);
                LOG.info("lzyFS::slot added " + slot.name());
            }
        });

        final Environment environment = EnvironmentFactory.create(
            context.env(),
            BaseEnvConfig.newBuilder().build()
        );
        try {
            environment.prepare();
        } catch (EnvironmentInstallationException e){
            Set.copyOf(slots.values()).stream().filter(s -> s instanceof LzyInputSlot).forEach(LzySlot::suspend);
            Set.copyOf(slots.values()).stream()
                .filter(s -> s instanceof LzyOutputSlot)
                .map(s -> (LzyOutputSlot)s)
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
        return environment;
    }

    public LzyExecution execute(AtomicZygote zygote, Environment environment, Consumer<ExecutionProgress> onProgress)
        throws LzyExecutionException {

        Map<String, String> envMap = System.getenv();
        List<String> envList = envMap.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());
        if (zygote.env() instanceof PythonEnv) {
            try {
                LinkedHashMap<String, String> localModules = new LinkedHashMap<>();
                ((PythonEnv) zygote.env()).localModules().forEach(localModule -> localModules.put(localModule.name(), localModule.uri()));
                envList.add("LOCAL_MODULES=" + new ObjectMapper().writeValueAsString(localModules));
                if (credentials.hasAmazon()) {
                    envList.add("AMAZON=" + JsonFormat.printer().print(credentials.getAmazon()));
                } else if (credentials.hasAzure()) {
                    envList.add("AZURE=" + JsonFormat.printer().print(credentials.getAzure()));
                } else {
                    envList.add("AZURE_SAS=" + JsonFormat.printer().print(credentials.getAzureSas()));
                }
            } catch (JsonProcessingException | InvalidProtocolBufferException e) {
                throw new LzyExecutionException(e);
            }
        }

        LzyExecution execution = new LzyExecution(taskId, zygote, arguments, envList.toArray(String[]::new));
        execution.onProgress(onProgress);
        execution.start(environment);
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
                .map(s -> (LzyOutputSlot)s)
                .forEach(LzyOutputSlot::forceClose);
        }
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
                            return new InFileSlot(taskId, spec, snapshotter.snapshotProvider());
                        case OUTPUT:
                            if (spec.name().startsWith("local://")) {
                                return new LocalOutFileSlot(taskId, spec, URI.create(spec.name()), snapshotter.snapshotProvider());
                            }
                            return new OutFileSlot(taskId, spec, snapshotter.snapshotProvider());
                    }
                    break;
                }
                case ARG:
                    arguments = binding;
                    return new LzySlotBase(spec, snapshotter.snapshotProvider()) {};
            }
            throw new UnsupportedOperationException("Not implemented yet");
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

    public void waitForSlots() throws InterruptedException {
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
