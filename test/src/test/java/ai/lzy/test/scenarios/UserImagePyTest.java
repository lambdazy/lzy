package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.PythonContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;

public class UserImagePyTest {
    static final Logger LOG = LogManager.getLogger(UserImagePyTest.class);

    @Rule
    public final ApplicationContextRule ctx = new ApplicationContextRule();

    @Rule
    public final ContextRule<PythonContext> pythonContext = new ContextRule<>(ctx, PythonContext.class);

    @Test
    public void testUserImage() {
        /* This scenario checks for:
                1. Execution is running inside container from user image
                2. External modules are available inside container
         */
        pythonContext.context().evalAndAssertScenarioResult("user_image_cpu");
    }

}
