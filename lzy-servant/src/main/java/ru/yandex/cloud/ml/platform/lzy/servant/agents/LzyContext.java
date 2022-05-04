package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.SlotsManager;
import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvironmentFactory;
import ru.yandex.cloud.ml.platform.lzy.slots.ArgumentsSlot;
import ru.yandex.cloud.ml.platform.lzy.slots.LineReaderSlot;
import ru.yandex.cloud.ml.platform.lzy.SlotConnectionManager;
import ru.yandex.cloud.ml.platform.lzy.slots.WriterSlot;
import ru.yandex.cloud.ml.platform.lzy.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ServantProgress;

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
    private final StorageClient storage;
    private final String mountRoot;
    private String arguments = "";
    private Environment env;

    public LzyContext(String contextId, SlotsManager slotsManager, SlotConnectionManager slotConnectionManager,
                      String mountRoot) {
        this.contextId = contextId;
        this.slotsManager = slotsManager;
        this.storage = slotConnectionManager.snapshooter().storage();
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

    public synchronized void prepare(Env from) throws EnvironmentInstallationException {
        env = EnvironmentFactory.create(from, storage);
    }

    public LzyExecution execute(String taskId, AtomicZygote zygote, Consumer<ServantProgress> onProgress)
        throws LzyExecutionException, InterruptedException {
        final long start = System.currentTimeMillis();
        if (env == null) {
            throw new LzyExecutionException(new RuntimeException("Cannot execute before prepare"));
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
