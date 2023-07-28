package ai.lzy.test;

import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.test.context.PythonContextTests;

public class RunAllServices {
    public static void main(String[] args) {
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration

        var pythonCtx = PythonContextTests.of();

        try {
            pythonCtx.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                pythonCtx.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
