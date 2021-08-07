package ru.yandex.cloud.ml.platform.lzy.servant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.InFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.LineReaderSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.OutFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.WriterSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
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
    private final WriterSlot stdinSlot;
    private Process exec;
    private final Map<String, LzySlot> slots = new HashMap<>();
    private List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();

    public LzyExecution(String taskId, AtomicZygote zygote) {
        this.taskId = taskId;
        this.zygote = zygote;
        stdinSlot = new WriterSlot(taskId, new TextLinesOutSlot(zygote, "/dev/stdin"));
        stdoutSlot = new LineReaderSlot(taskId, new TextLinesOutSlot(zygote, "/dev/stdout"));
        stderrSlot = new LineReaderSlot(taskId, new TextLinesOutSlot(zygote, "/dev/stderr"));
        if (zygote != null) {
            zygote.slots().forEach(spec -> configureSlot(spec, null));
            slots.put("/dev/stdin", stdinSlot);
            slots.put("/dev/stdout", stdoutSlot);
            slots.put("/dev/stderr", stderrSlot);
        }
    }

    public LzySlot configureSlot(Slot spec, String channelId) {
        try {
            LzySlot slot = null;
            switch (spec.media()) {
                case PIPE:
                case FILE: {
                    switch (spec.direction()) {
                        case INPUT:
                            slot = new InFileSlot(taskId, spec);
                            break;
                        case OUTPUT:
                            slot = new OutFileSlot(taskId, spec);
                            break;
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Not implemented yet");
            }
            if (slots.containsKey(spec.name())) {
                return null;
            }
            slots.put(spec.name(), slot);
            final Servant.AttachSlot.Builder attachBuilder = Servant.AttachSlot.newBuilder()
                .setChannel(channelId != null ? channelId : "")
                .setSlot(gRPCConverter.to(spec));
            progress(Servant.ExecutionProgress.newBuilder().setAttached(attachBuilder.build()).build());
            return slot;
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void start() {
        try {
            progress(Servant.ExecutionProgress.newBuilder()
                .setStarted(Servant.ExecutionStarted.newBuilder().build())
                .build()
            );
            exec = Runtime.getRuntime().exec(zygote.fuze());
            exec.onExit().thenAcceptAsync(p ->
                progress(Servant.ExecutionProgress.newBuilder()
                    .setExit(Servant.ExecutionConcluded.newBuilder().setRc(p.exitValue()).build())
                    .build()
                ));
            stdinSlot.setStream(new OutputStreamWriter(exec.getOutputStream(), StandardCharsets.UTF_8));
            stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
                exec.getInputStream(),
                StandardCharsets.UTF_8
            )));
            stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
                exec.getErrorStream(),
                StandardCharsets.UTF_8
            )));
        }
        catch (IOException e) {
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
