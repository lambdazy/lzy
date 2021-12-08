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
import java.util.Map;
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
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvFactory;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment;
import ru.yandex.cloud.ml.platform.lzy.servant.env.Environment.LzyProcess;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.*;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import java.util.*;
import java.util.function.Consumer;

@SuppressWarnings("WeakerAccess")
public class LzyExecution {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final String taskId;
    private final AtomicZygote zygote;
    private LzyProcess exec; // lzyProcess
    private final String arguments;
    private final List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private final String[] envp;

    public LzyExecution(String taskId, AtomicZygote zygote, String arguments, String[] envp) {
        this.taskId = taskId;
        this.zygote = zygote;
        this.arguments = arguments;
        this.envp = envp;
    }

    public void start(Environment session) throws LzyExecutionException {
        // TODO (lindvv)
        // Environment environment = EnvFactory.create(zygote, new EnvConfig())
        final long startMillis = System.currentTimeMillis();
        if (zygote == null) {
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        } else if (exec != null) {
            throw new IllegalStateException("LzyExecution has been already started");
        }
        final long envExecFinishMillis;
        progress(Servant.ExecutionProgress.newBuilder()
            .setStarted(Servant.ExecutionStarted.newBuilder().build())
            .build()
        );

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
            this.exec = session.exec(command, envp);
                /* TODO (lindvv)
                stdinSlot.setStream(new OutputStreamWriter(lzyProcess.in(), StandardCharsets.UTF_8));
                stdoutSlot.setStream(new LineNumberReader(new InputStreamReader(
                    lzyProcess.out(),
                    StandardCharsets.UTF_8
                )));
                stderrSlot.setStream(new LineNumberReader(new InputStreamReader(
                    lzyProcess.err(),
                    StandardCharsets.UTF_8
                )));
                */
            UserEventLogger.log(new UserEvent(
                "Servant execution start",
                Map.of(
                    "task_id", taskId,
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionStart
            ));

        } catch (LzyExecutionException e) {
            resultDescription = "Error during task execution:\n" + e;
            rc = ReturnCodes.EXECUTION_ERROR.getRc();
            LOG.info("Result description: " + resultDescription);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder()
                    .setRc(rc)
                    .setDescription(resultDescription)
                    .build())
                .build()
            );
            throw e;
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
    }

    public synchronized void progress(Servant.ExecutionProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<Servant.ExecutionProgress> listener) {
        listeners.add(listener);
    }

    public int waitFor() {
        try {
            int rc = exec.waitFor();
            String resultDescription = (rc == 0) ? "Success" : "Failure";
            LOG.info("Result description: " + resultDescription);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder()
                    .setRc(rc)
                    .setDescription(resultDescription)
                    .build())
                .build()
            );
            return rc;
        } catch (Exception e) {
            final String exceptionDescription = "Exception during task execution" + e.getMessage() +
                "\n\nStackTrace: " + Arrays.toString(e.getStackTrace());
            LOG.warn(exceptionDescription);
            progress(Servant.ExecutionProgress.newBuilder()
                .setExit(Servant.ExecutionConcluded.newBuilder()
                    .setRc(-1)
                    .setDescription(exceptionDescription)
                    .build())
                .build()
            );
            return -1;
        }
    }

    public void signal(int sigValue) {
        if (lzyProcess == null) {
            LOG.warn("Attempt to kill not started process");
        }
        try {
            lzyProcess.signal(sigValue);
        } catch (Exception e) {
            LOG.warn("Unable to send signal to process", e);
        }
    }

    @SuppressWarnings("unused")
    public Zygote zygote() {
        return zygote;
    }

    public Process exec(){
        return exec;
    }
}
