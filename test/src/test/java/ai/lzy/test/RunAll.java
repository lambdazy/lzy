package ai.lzy.test;

import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.test.context.PythonContextTests;
import org.junit.Test;

public class RunAll extends PythonContextTests {
    static {
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration
    }

    @Test
    public void test() throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
        }
    }
}
