package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.ReturnCodes;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.env.CondaEnvironment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.SimpleBashEnvironment;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.*;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class LzyExecution {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final String taskId;
    private final AtomicZygote zygote;
    private final LineReaderSlot stdoutSlot;
    private final LineReaderSlot stderrSlot;
    private final URI servantUri;
    private final WriterSlot stdinSlot;
    private Process exec;
    private String arguments = "";
    private final Map<String, LzySlot> slots = new ConcurrentHashMap<>();
    private final List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private final LockManager lockManager = new LocalLockManager();
    private final Snapshotter snapshotter;
    /* temporary bad solution; will go away */
    private final Lzy.GetS3CredentialsResponse credentials;

    public LzyExecution(String taskId, AtomicZygote zygote, URI servantUri, Snapshotter snapshotter, Lzy.GetS3CredentialsResponse credentials) {
        this.taskId = taskId;
        this.zygote = zygote;
        stdinSlot = new WriterSlot(taskId, new TextLinesInSlot("/dev/stdin"), snapshotter.snapshotProvider());
        stdoutSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stdout"), snapshotter.snapshotProvider());
        stderrSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stderr"), snapshotter.snapshotProvider());
        this.servantUri = servantUri;
        this.snapshotter = snapshotter;
        this.credentials = credentials;
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
                        if (zygote != null || spec.direction() == Slot.Direction.INPUT) {
                            progress(Servant.ExecutionProgress.newBuilder()
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
                        LOG.info("LzyExecution::Slots.remove(\n" + slot.name() + "\n)");
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
                progress(Servant.ExecutionProgress.newBuilder().setAttach(
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

    public void start() {
        final long startMillis = System.currentTimeMillis();
        if (zygote == null) {
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        } else if (exec != null) {
            throw new IllegalStateException("LzyExecution has been already started");
        }
        final long envExecFinishMillis;
        final long slotsClosedMillis;
        try {
            progress(Servant.ExecutionProgress.newBuilder()
                .setStarted(Servant.ExecutionStarted.newBuilder().build())
                .build()
            );
            Environment session;
            if (zygote.env() instanceof PythonEnv) {
                session = new CondaEnvironment((PythonEnv) zygote.env());
                LOG.info("Conda environment is provided, using CondaEnvironment");
            } else {
                session = new SimpleBashEnvironment();
                LOG.info("No environment provided, using SimpleBashEnvironment");
            }

            String command = zygote.fuze() + " " + arguments;
            LOG.info("Going to exec command " + command);
            int rc;
            String resultDescription;
            final long envExecStartMillis = System.currentTimeMillis();
            try {
                MetricEventLogger.log(
                    new MetricEvent(
                        "time from LzyExecution::start to Environment::exec",
                        Map.of("metric_type", "system_metric"),
                        envExecStartMillis - startMillis
                    )
                );
                Map<String, String> envMap = System.getenv();
                List<String> envList = envMap.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.toList());
                if (zygote.env() instanceof PythonEnv) {
                    try {
                        envList.add("LOCAL_MODULES=" + new ObjectMapper().writeValueAsString(((PythonEnv) zygote.env()).localModules()));
                        if (credentials.hasAmazon()) {
                            envList.add("AMAZON=" + JsonFormat.printer().print(credentials.getAmazon()));
                            System.out.println("SSSSSS " + JsonFormat.printer().print(credentials.getAmazon()));
                        } else if (credentials.hasAzure()) {
                            envList.add("AZURE=" + JsonFormat.printer().print(credentials.getAzure()));
                        } else {
                            envList.add("AZURE_SAS=" + JsonFormat.printer().print(credentials.getAzureSas()));
                        }
                    } catch (JsonProcessingException | InvalidProtocolBufferException e) {
                        throw new EnvironmentInstallationException(e.getMessage());
                    }
                }
                this.exec = session.exec(command, envList.toArray(String[]::new));
                stdinSlot.setStream(new OutputStreamWriter(exec.getOutputStream(), StandardCharsets.UTF_8));
                stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
                    exec.getInputStream(),
                    StandardCharsets.UTF_8
                )));
                stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
                    exec.getErrorStream(),
                    StandardCharsets.UTF_8
                )));
                UserEventLogger.log(new UserEvent(
                    "Servant execution start",
                    Map.of(
                        "task_id", taskId,
                        "zygote_description", zygote.description()
                    ),
                    UserEvent.UserEventType.ExecutionStart
                ));
                rc = exec.waitFor();
                resultDescription = (rc == 0) ? "Success" : "Failure";
            } catch (EnvironmentInstallationException e) {
                resultDescription = "Error during environment installation:\n" + e;
                rc = ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc();
            } catch (LzyExecutionException e) {
                resultDescription = "Error during task execution:\n" + e;
                rc = ReturnCodes.EXECUTION_ERROR.getRc();
            } finally {
                envExecFinishMillis = System.currentTimeMillis();
                MetricEventLogger.log(
                    new MetricEvent(
                        "env execution time",
                        Map.of("metric_type", "task_metric"),
                        envExecFinishMillis - envExecStartMillis
                    )
                );
            }

            Set.copyOf(slots.values()).stream().filter(s -> s instanceof LzyInputSlot).forEach(LzySlot::suspend);
            if (rc != 0) {
                Set.copyOf(slots.values()).stream()
                    .filter(s -> s instanceof LzyOutputSlot)
                    .map(s -> (LzyOutputSlot)s)
                    .forEach(LzyOutputSlot::forceClose);
            }
            synchronized (slots) {
                LOG.info("Slots: " + Arrays.toString(slots().map(LzySlot::name).toArray()));
                while (!slots.isEmpty()) {
                    slots.wait();
                }
            }
            slotsClosedMillis = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "time from env exec finished to slots closed",
                    Map.of("metric_type", "system_metric"),
                    slotsClosedMillis - envExecFinishMillis
                )
            );
            LOG.info("Result description: " + resultDescription);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder()
                    .setRc(rc)
                    .setDescription(resultDescription)
                    .build())
                .build()
            );
            final long finishMillis = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "time from slots closed to LzyExecution::start finish",
                    Map.of("metric_type", "system_metric"),
                    finishMillis - slotsClosedMillis
                )
            );
        } catch (InterruptedException e) {
            final String exceptionDescription = "InterruptedException during task execution" + e;
            LOG.warn(exceptionDescription);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder()
                    .setRc(-1)
                    .setDescription(exceptionDescription)
                    .build())
                .build()
            );
        }
    }

    public Stream<LzySlot> slots() {
        return slots.values().stream();
    }

    public synchronized void progress(Servant.ExecutionProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<Servant.ExecutionProgress> listener) {
        listeners.add(listener);
    }

    public LzySlot slot(String name) {
        return slots.get(name);
    }

    public void signal(int sigValue) {
        if (exec == null) {
            LOG.warn("Attempt to kill not started process");
        }
        try {
            Runtime.getRuntime().exec("kill -" + sigValue + " " + exec.pid());
        } catch (Exception e) {
            LOG.warn("Unable to send signal to process", e);
        }
    }

    @SuppressWarnings("unused")
    public Zygote zygote() {
        return zygote;
    }
}
