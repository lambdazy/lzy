package ai.lzy.worker.env;

import ai.lzy.worker.StreamQueue;

import java.io.InputStream;
import java.io.OutputStream;

public interface Environment extends AutoCloseable {

    /**
     * Install environment. Must be called before execution
     * Consumes stream queues to add stdout and stderr to
     */
    void install(StreamQueue.LogHandle logHandle) throws EnvironmentInstallationException;

    LzyProcess runProcess(String... command);

    LzyProcess runProcess(String[] command, String[] envp);

    default LzyProcess runProcess(String command, String[] envp) {
        return runProcess(new String[]{command}, envp);
    }

    interface LzyProcess {

        LzyProcess EMPTY = new LzyProcess() {
            @Override
            public OutputStream in() {
                return OutputStream.nullOutputStream();
            }

            @Override
            public InputStream out() {
                return InputStream.nullInputStream();
            }

            @Override
            public InputStream err() {
                return InputStream.nullInputStream();
            }

            @Override
            public int waitFor() {
                return 0;
            }

            @Override
            public void signal(int sigValue) {}
        };

        OutputStream in();

        InputStream out();

        InputStream err();

        int waitFor() throws InterruptedException;

        void signal(int sigValue);
    }
}
