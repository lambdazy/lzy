package ai.lzy.servant.agents;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ai.lzy.servant.env.Environment;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuppressWarnings("WeakerAccess")
public class LzyExecution {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final String taskId;
    private final AtomicZygote zygote;
    private final String arguments;
    private final List<Consumer<Servant.ServantProgress>> listeners = new ArrayList<>();
    private final String lzyMount;
    private Environment.LzyProcess process;

    public LzyExecution(String taskId, AtomicZygote zygote, String arguments, String lzyMount) {
        this.taskId = taskId;
        this.zygote = zygote;
        this.arguments = arguments;
        this.lzyMount = lzyMount;
    }

    public void start(Environment environment) {
        final long startMillis = System.currentTimeMillis();
        if (zygote == null) {
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        } else if (process != null) {
            throw new IllegalStateException("LzyExecution has been already started");
        }
        final long envExecFinishMillis;
        progress(Servant.ServantProgress.newBuilder()
            .setExecuteStart(Servant.ExecutionStarted.newBuilder().build())
            .build()
        );

        final String command = zygote.fuze() + " " + arguments;
        LOG.info("Going to exec command " + command);
        final long envExecStartMillis = System.currentTimeMillis();
        try {
            MetricEventLogger.log(
                new MetricEvent(
                    "time from LzyExecution::start to Environment::exec",
                    Map.of("metric_type", "system_metric"),
                    envExecStartMillis - startMillis
                )
            );
            this.process = environment.runProcess(command, new String[]{"LZY_MOUNT=" + lzyMount});
            UserEventLogger.log(new UserEvent(
                "Servant execution start",
                Map.of(
                    "task_id", taskId,
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionStart
            ));


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

    public synchronized void progress(Servant.ServantProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    public synchronized void onProgress(Consumer<Servant.ServantProgress> listener) {
        listeners.add(listener);
    }

    public void waitFor() {
        int rc = process.waitFor();
        String resultDescription = (rc == 0) ? "Success" : "Failure";
        LOG.info("Result description: " + resultDescription);
        progress(Servant.ServantProgress.newBuilder()
            .setExecuteStop(Servant.ExecutionConcluded.newBuilder()
                .setRc(rc)
                .setDescription(resultDescription)
                .build())
            .build()
        );
    }

    public void signal(int sigValue) {
        if (process == null) {
            LOG.warn("Attempt to kill not started process");
        }
        try {
            process.signal(sigValue);
        } catch (Exception e) {
            LOG.warn("Unable to send signal to process", e);
        }
    }

    @SuppressWarnings("unused")
    public Zygote zygote() {
        return zygote;
    }

    public Environment.LzyProcess process() {
        return process;
    }
}
