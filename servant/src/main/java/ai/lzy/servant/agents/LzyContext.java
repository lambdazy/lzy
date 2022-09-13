package ai.lzy.servant.agents;

import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.ArgumentsSlot;
import ai.lzy.fs.slots.LineReaderSlot;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.EnvironmentInstallationException;
import ai.lzy.model.graph.Env;
import ai.lzy.v1.deprecated.Servant;
import ai.lzy.v1.deprecated.Servant.ServantProgress;
import ai.lzy.servant.env.Environment;
import ai.lzy.servant.env.EnvironmentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
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

    public LzySlot getOrCreateSlot(String task, Slot spec, String binding) {
        final LzySlot slot = slotsManager.getOrCreateSlot(task, spec, binding);

        if (slot instanceof ArgumentsSlot) {
            arguments = ((ArgumentsSlot) slot).getArguments();
        }

        return slot;
    }

    public synchronized void prepare(Env from, StorageClient storage) throws EnvironmentInstallationException {
        env = EnvironmentFactory.create(from, storage);
    }

    public LzyExecution execute(String taskId, String command, Consumer<ServantProgress> onProgress) {
        if (env == null) {
            LOG.error("env is null before execution");
            throw new IllegalStateException("Cannot execute before prepare");
        }

        final LzyExecution execution = new LzyExecution(contextId, command, arguments, mountRoot);
        final LineReaderSlot stdoutSlot = (LineReaderSlot) getOrCreateSlot(taskId, Slot.STDOUT, null);
        final LineReaderSlot stderrSlot = (LineReaderSlot) getOrCreateSlot(taskId, Slot.STDERR, null);
        execution.onProgress(progress -> {
            slotsManager.reportProgress(progress);
            onProgress.accept(progress);
        });
        execution.start(env);
        stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.process().out(),
            StandardCharsets.UTF_8
        )));
        stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
            execution.process().err(),
            StandardCharsets.UTF_8
        )));
        try {
            execution.process().in().close();
        } catch (IOException e) {
            // ignore
        }
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
