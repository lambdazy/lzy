package ai.lzy.test.scenarios;

import ai.lzy.service.workflow.WorkflowService;
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

    static {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = true;  // To recreate portals for all wfs
    }

    @Test
    public void test() {
        pythonContext.context().evalAndAssertScenarioResult("custom-image-cpu");
    }

}
