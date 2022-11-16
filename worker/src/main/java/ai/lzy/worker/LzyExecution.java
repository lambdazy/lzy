package ai.lzy.worker;

import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.logs.UserEvent;
import ai.lzy.logs.UserEventLogger;
import ai.lzy.worker.env.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

@SuppressWarnings("WeakerAccess")
public class LzyExecution {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final String taskId;
    private final String command;
    private final String arguments;
    private final String lzyMount;
    private Environment.LzyProcess process;

    public LzyExecution(String taskId, String command, String arguments, String lzyMount) {
        this.taskId = taskId;
        this.command = command;
        this.arguments = arguments;
        this.lzyMount = lzyMount;
    }

    // TODO(artolord) remove progress
    public void start(Environment environment) {
        final long startMillis = System.currentTimeMillis();
        if (command == null) {
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        } else if (process != null) {
            throw new IllegalStateException("LzyExecution has been already started");
        }
        final long envExecFinishMillis;

        final String command = this.command + " " + arguments;
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
                    "task_id", taskId
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

    public int waitFor() {
        int rc = process.waitFor();
        String resultDescription = (rc == 0) ? "Success" : "Failure";
        LOG.info("Result description: " + resultDescription);
        return rc;
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

    public Environment.LzyProcess process() {
        return process;
    }
}
