package ai.lzy.test.scenarios;

import ai.lzy.servant.env.CondaEnvironment;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.PythonContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

public class PyApiTest {
    static final Logger LOG = LogManager.getLogger(SchedulerTest.class);

    @ClassRule
    public static final ApplicationContextRule ctx = new ApplicationContextRule();

    @ClassRule
    public static final ContextRule<PythonContext> pythonContext = new ContextRule<>(ctx, PythonContext.class);

    static {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = true;  // To recreate portals for all wfs
        CondaEnvironment.RECONFIGURE_CONDA = false;  // To optimize conda configuration
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        pythonContext.context().evalAndAssertScenarioResult("catboost_integration_cpu", List.of("catboost"));
    }

    @Test
    public void testExecFail() {
        pythonContext.context().evalAndAssertScenarioResult("exec_fail");
    }

    @Test
    public void testEnvFail() {
        CondaEnvironment.RECONFIGURE_CONDA = true;
        pythonContext.context().evalAndAssertScenarioResult("env_fail");
        CondaEnvironment.RECONFIGURE_CONDA = false;
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */
        pythonContext.context().evalAndAssertScenarioResult("import");
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        pythonContext.context().evalAndAssertScenarioResult("none_result");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
                TODO: turn on after new whiteboards finished
         */
        pythonContext.context().evalAndAssertScenarioResult("whiteboards");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        pythonContext.context().evalAndAssertScenarioResult("file_test");
    }
}
