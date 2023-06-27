package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.PythonContext;
import ai.lzy.worker.env.CondaEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

public class RunAll {
    @ClassRule
    public static final ApplicationContextRule ctx = new ApplicationContextRule();

    @ClassRule
    public static final ContextRule<PythonContext> pythonContext = new ContextRule<>(ctx, PythonContext.class);

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
