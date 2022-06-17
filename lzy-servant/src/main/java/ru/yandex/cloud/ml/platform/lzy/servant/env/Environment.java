package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.InputStream;
import java.io.OutputStream;

public interface Environment extends AutoCloseable {
    LzyProcess runProcess(String... command);

    LzyProcess runProcess(String[] command, String[] envp);

    default LzyProcess runProcess(String command, String[] envp) {
        return runProcess(new String[]{command}, envp);
    }

    interface LzyProcess {
        OutputStream in();

        InputStream out();

        InputStream err();

        int waitFor();

        void signal(int sigValue);
    }
}
