package ai.lzy.test.scenarios;

import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.PythonContext;
import ai.lzy.worker.env.CondaEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

public class ExceptionTest {
    @ClassRule
    public static final ApplicationContextRule ctx = new ApplicationContextRule();

    @ClassRule
    public static final ContextRule<PythonContext> pythonContext = new ContextRule<>(ctx, PythonContext.class);

    static {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = true;  // To recreate portals for all wfs
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration
    }

    @Test
    public void testExceptionSerialize() {
        pythonContext.context().evalAndAssertScenarioResult("exception_serialize");
    }
}
