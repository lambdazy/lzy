package ai.lzy.servant.agents;

import ai.lzy.servant.env.EnvironmentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.Slot;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.graph.Env;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.servant.env.Environment;
import ai.lzy.fs.slots.ArgumentsSlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.fs.slots.WriterSlot;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.priv.v2.Servant;
import ai.lzy.priv.v2.Servant.ServantProgress;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * This class is a monitor since all operations with the context must be sequential
 */
public class LzyContext implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(LzyContext.class);

    private final SlotsManager slotsManager;
    private final String contextId;
    private final String mountRoot;
    private String arguments = "";
    private Environment env;

    public LzyContext(String contextId, SlotsManager slotsManager,
                      String mountRoot) {
        this.contextId = contextId;
        this.slotsManager = slotsManager;
        this.mountRoot = mountRoot;
    }

    public void start() {
        slotsManager.reportProgress(Servant.ServantProgress.newBuilder()
            .setStart(Servant.Started.newBuilder().build())
            .build());
    }

    public Stream<LzySlot> slots() {
        return slotsManager.slots();
    }

    public LzySlot slot(String task, String name) {
        return slotsManager.slot(task, name);
    }

    public LzySlot configureSlot(String task, Slot spec, String binding) {
        final LzySlot slot = slotsManager.configureSlot(task, spec, binding);

        if (slot instanceof ArgumentsSlot) {
            arguments = ((ArgumentsSlot) slot).getArguments();
        }

        return slot;
    }

    public synchronized void prepare(Env from, StorageClient storage) throws EnvironmentInstallationException {
        env = EnvironmentFactory.create(from, storage);
    }

    public LzyExecution execute(String taskId, AtomicZygote zygote, Consumer<ServantProgress> onProgress) {
        final long start = System.currentTimeMillis();
        if (env == null) {
            LOG.error("env is null before execution");
            throw new IllegalStateException("Cannot execute before prepare");
        }

        final LzyExecution execution = new LzyExecution(contextId, zygote, arguments, mountRoot);
        final WriterSlot stdinSlot = (WriterSlot) configureSlot(taskId, Slot.STDIN, null);
        final LineReaderSlot stdoutSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDOUT, null);
        final LineReaderSlot stderrSlot = (LineReaderSlot) configureSlot(taskId, Slot.STDERR, null);
        execution.onProgress(progress -> {
            slotsManager.reportProgress(progress);
            onProgress.accept(progress);
        });
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


    public void onProgress(Consumer<ServantProgress> listener) {
        slotsManager.onProgress(listener);
    }

    @Override
    public void close() {
        try {
            slotsManager.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
