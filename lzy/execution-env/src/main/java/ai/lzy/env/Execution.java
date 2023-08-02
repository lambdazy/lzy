package ai.lzy.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("WeakerAccess")
public class Execution {
    private static final Logger LOG = LogManager.getLogger(Execution.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final String command;
    private final String arguments;
    private Environment.LzyProcess process;

    public Execution(String command, String arguments) {
        this.command = command;
        this.arguments = arguments;
    }

    public void start(Environment environment) {
        if (command == null) {
            throw new IllegalStateException("Unable to start execution while in terminal mode");
        } else if (process != null) {
            throw new IllegalStateException("LzyExecution has been already started");
        }

        final String command = this.command + " " + arguments;
        LOG.info("Going to exec command " + command);

        this.process = environment.runProcess(command);
    }

    public int waitFor() throws InterruptedException {
        int rc = process.waitFor();
        String resultDescription = (rc == 0)
            ? "Success"
            : "Error while executing command on worker, rc = " + rc + ".\nSee your stdout/stderr to see more info.";
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
