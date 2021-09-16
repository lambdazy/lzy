package ru.yandex.cloud.ml.platform.lzy.servant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.InFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LineReaderSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LzySlotBase;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.OutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.WriterSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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
    private final Map<String, LzySlot> slots = new HashMap<>();
    private List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();

    public LzyExecution(String taskId, AtomicZygote zygote, URI servantUri) {
        this.taskId = taskId;
        this.zygote = zygote;
        stdinSlot = new WriterSlot(taskId, new TextLinesInSlot("/dev/stdin"));
        stdoutSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stdout"));
        stderrSlot = new LineReaderSlot(taskId, new TextLinesOutSlot("/dev/stderr"));
        this.servantUri = servantUri;
    }

    public LzySlot configureSlot(Slot spec, String binding) {
        if (slots.containsKey(spec.name()))
            return slots.get(spec.name());
        try {
            final LzySlot slot = createSlot(spec, binding);
            if (slot.state() != Operations.SlotStatus.State.CLOSED)
                slots.put(spec.name(), slot);

            slot.onState(Operations.SlotStatus.State.CLOSED, () -> {
                progress(Servant.ExecutionProgress.newBuilder()
                    .setDetach(Servant.SlotDetach.newBuilder()
                        .setSlot(gRPCConverter.to(spec))
                        .setUri(servantUri.toString() + spec.name())
                        .build()
                    ).build()
                );
                synchronized (slots) {
                    slots.remove(slot.name());
                    slots.notifyAll();
                }
            });
            if (binding == null)
                binding = "";
            else if (binding.startsWith("channel:"))
                binding = binding.substring("channel:".length());
            progress(Servant.ExecutionProgress.newBuilder().setAttach(
                Servant.SlotAttach.newBuilder()
                    .setChannel(binding)
                    .setSlot(gRPCConverter.to(spec))
                    .setUri(servantUri.toString() + spec.name())
                    .build()
            ).build());
            LOG.info("Configured slot " + spec.name() + " " + slot);
            return slot;
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public LzySlot createSlot(Slot spec, String binding) throws IOException {
        if (spec.equals(Slot.STDIN))
            return stdinSlot;
        else if (spec.equals(Slot.STDOUT))
            return stdoutSlot;
        else if (spec.equals(Slot.STDERR))
            return stderrSlot;

        switch (spec.media()) {
            case PIPE:
            case FILE: {
                switch (spec.direction()) {
                    case INPUT:
                        return new InFileSlot(taskId, spec);
                    case OUTPUT:
                        return new OutFileSlot(taskId, spec);
                }
                break;
            }
            case ARG:
                arguments = binding;
                return new LzySlotBase(spec){};
        }
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public void start() {
        if (zygote == null)
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        try {
            progress(Servant.ExecutionProgress.newBuilder()
                .setStarted(Servant.ExecutionStarted.newBuilder().build())
                .build()
            );
            exec = Runtime.getRuntime().exec(zygote.fuze() + " " + arguments);

            stdinSlot.setStream(new OutputStreamWriter(exec.getOutputStream(), StandardCharsets.UTF_8));
            stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
                exec.getInputStream(),
                StandardCharsets.UTF_8
            )));
            stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
                exec.getErrorStream(),
                StandardCharsets.UTF_8
            )));
            final int rc = exec.waitFor();
            LOG.info("Slots: " + Arrays.toString(slots().map(LzySlot::name).toArray()));
            Set.copyOf(slots.values()).stream().filter(s -> s instanceof LzyInputSlot).forEach(LzySlot::close);
            synchronized (slots) {
                while (!slots.isEmpty()) {
                    slots.wait();
                }
            }
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder().setRc(rc).build())
                .build()
            );
        }
        catch (IOException | InterruptedException e) {
            LOG.warn("Exception during task execution", e);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder().setRc(-1).build())
                .build()
            );
        }
    }

    public Stream<LzySlot> slots() {
        return slots.values().stream();
    }

    public void progress(Servant.ExecutionProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    public void onProgress(Consumer<Servant.ExecutionProgress> listener) {
        listeners.add(listener);
    }

    public LzySlot slot(String name) {
        return slots.get(name);
    }

    public void signal(int sigValue) {
        try {
            Runtime.getRuntime().exec("kill -" + sigValue + " " + exec.pid());
        } catch (IOException e) {
            LOG.warn("Unable to send signal to process", e);
        }
    }

    public Zygote zygote() {
        return zygote;
    }
}
